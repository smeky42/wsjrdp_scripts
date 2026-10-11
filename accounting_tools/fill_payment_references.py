#!/usr/bin/env -S uv run
"""Fill the stock of payment codes (wsjrdp_payment_references).

Makes new payment codes with the issuing rules of docs/payment_code.md --
each codeword at a Hamming distance of at least 8 and a Levenshtein distance
of at least 4 to every codeword already in the table, the ones made in this
run included -- and inserts them as ``available`` (origin ``script``). The
Hitobito wagon hands them out to payment notices.

How many: ``--fill-to N`` (default 500) makes as many as are missing for N
available codes; ``--count N`` makes exactly N.

The codes are made on the read-only connection and shown before anything is
written. After the approval the read/write transaction takes the advisory
lock the wagon takes when it makes codes (2027000001), reads the codewords
again and checks the new codes against those that appeared in the meantime;
a code that comes too close aborts the run. ``--dry-run`` only shows the
plan, ``--rollback-for-testing`` inserts and rolls back.

``--benchmark`` only makes codes and touches no database: for each size in
``--benchmark-sizes`` it fills an in-memory set with that many random
codewords (without the distance rules, which is fast) and then makes up to
``--benchmark-count`` codes against it, within ``--benchmark-seconds`` per
size. It reports the time per 1000 codes, the draws per code and how many
codes fell back to the farthest draw because no draw kept the distances.
"""

from __future__ import annotations

import logging as _logging
import pathlib as _pathlib
import secrets as _secrets
import sys as _sys
import time as _time
import typing as _typing

import wsjrdp2027


if _typing.TYPE_CHECKING:
    import psycopg as _psycopg


_SELF_NAME = _pathlib.Path(__file__).stem
_LOGGER = _logging.getLogger(__name__)

_TABLE_NAME = "wsjrdp_payment_references"
# The advisory lock every maker of codes takes (the wagon:
# WsjrdpPaymentReference::GENERATION_LOCK_KEY).
GENERATION_LOCK_KEY = 2_027_000_001
POOL_SIZE = 500
LOW_STOCK = 100
DEFAULT_BENCHMARK_SIZES = "0,1000,5000,10000,20000,50000,100000,200000"

_COLUMNS = (
    "payment_code_type",
    "payment_code",
    "codeword",
    "codeword_scheme",
    "data_length",
    "parity_length",
    "rs_field_exponent",
    "rs_primitive_polynomial",
    "rs_generator",
    "rs_fcr",
    "status",
    "origin",
)


def count_to_make(available: int, *, fill_to: int | None, count: int | None) -> int:
    """How many codes to make: ``count`` as given, else what is missing for
    ``fill_to`` available codes.

    >>> count_to_make(120, fill_to=500, count=None)
    380
    >>> count_to_make(600, fill_to=500, count=None)
    0
    >>> count_to_make(600, fill_to=500, count=7)
    7
    """
    if count is not None:
        return count
    return max((fill_to if fill_to is not None else POOL_SIZE) - available, 0)


def row_for(code: str) -> tuple:
    """The values of one new available reference, in the order of _COLUMNS.

    >>> row_for("RF75K7M3QX9PNFK3")[:4]
    ('rf', 'RF75K7M3QX9PNFK3', 'K7M3QX9PNFK3', 'crockford_rs')
    >>> row_for("RF75K7M3QX9PNFK3")[4:]
    (8, 4, 5, 37, 2, 1, 'available', 'script')
    """
    pc = wsjrdp2027._payment_code
    return (
        "rf",
        code,
        pc.codeword_of(code),
        "crockford_rs",
        pc.DATA_LENGTH,
        pc.PARITY_LENGTH,
        5,
        pc.RS_PRIMITIVE_POLYNOMIAL,
        pc.RS_GENERATOR,
        pc.RS_FCR,
        "available",
        "script",
    )


def make_codes(issued: wsjrdp2027.IssuedCodewords, count: int) -> list[str]:
    """count new payment codes, each far enough from every codeword in
    issued and from the ones made before it (issued grows with them)."""
    pc = wsjrdp2027._payment_code
    codes: list[str] = []
    for _ in range(count):
        code = wsjrdp2027.generate_distinct_payment_code(issued)
        issued.add(pc.codeword_of(code))
        codes.append(code)
    return codes


def keeps_distances(issued: wsjrdp2027.IssuedCodewords, codeword: str) -> bool:
    """Whether codeword keeps the minimum distances to every codeword in issued."""
    pc = wsjrdp2027._payment_code
    hamming, levenshtein = issued.distances(codeword)
    return (
        hamming >= pc.MIN_HAMMING_DISTANCE
        and levenshtein >= pc.MIN_LEVENSHTEIN_DISTANCE
    )


def _read_codewords(conn: _psycopg.Connection) -> tuple[list[str], int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT codeword, status FROM wsjrdp_payment_references ORDER BY id"
        )
        rows = cur.fetchall()
    return [row[0] for row in rows], sum(1 for row in rows if row[1] == "available")


def run_benchmark(sizes: list[int], *, count: int, seconds: float) -> list[dict]:
    """Make codes against in-memory sets of the given sizes; no database."""
    pc = wsjrdp2027._payment_code
    results: list[dict] = []
    issued = wsjrdp2027.IssuedCodewords()
    filled = 0
    for size in sizes:
        while filled < size:
            issued.add(pc.generate_rs_codeword())
            filled += 1
        draws = 0

        def random_data() -> str:
            nonlocal draws
            draws += 1
            return "".join(_secrets.choice(pc.ALPHABET) for _ in range(pc.DATA_LENGTH))

        made = fallbacks = 0
        start = _time.perf_counter()
        elapsed = 0.0
        while made < count and elapsed < seconds:
            codeword = pc.generate_distinct_rs_codeword(issued, random_data=random_data)
            if not keeps_distances(issued, codeword):
                fallbacks += 1
            issued.add(codeword)
            filled += 1
            made += 1
            elapsed = _time.perf_counter() - start
        result = {
            "issued": size,
            "made": made,
            "seconds": elapsed,
            "seconds_per_1000": elapsed / made * 1000 if made else float("nan"),
            "draws_per_code": draws / made if made else float("nan"),
            "fallbacks": fallbacks,
        }
        _LOGGER.info(
            "[benchmark] %7d issued: %4d made in %6.1f s -> %8.1f s per 1000, "
            "%.2f draws per code, %d fell back to the farthest draw",
            result["issued"],
            result["made"],
            result["seconds"],
            result["seconds_per_1000"],
            result["draws_per_code"],
            result["fallbacks"],
        )
        results.append(result)
    return results


def create_argument_parser():
    import argparse

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = p.add_mutually_exclusive_group()
    group.add_argument(
        "--fill-to",
        type=int,
        default=POOL_SIZE,
        help=f"Make as many as are missing for this many available codes (default {POOL_SIZE}).",
    )
    group.add_argument(
        "--count", type=int, default=None, help="Make exactly this many codes."
    )
    p.add_argument(
        "--rollback-for-testing",
        action="store_true",
        default=False,
        help="Insert, then ROLLBACK instead of committing (testing).",
    )
    p.add_argument(
        "--benchmark",
        action="store_true",
        default=False,
        help="Only make codes against in-memory sets, no database.",
    )
    p.add_argument(
        "--benchmark-sizes",
        default=DEFAULT_BENCHMARK_SIZES,
        help=f"Comma-separated set sizes (default {DEFAULT_BENCHMARK_SIZES}).",
    )
    p.add_argument(
        "--benchmark-count",
        type=int,
        default=1000,
        help="Codes to make per size (default 1000).",
    )
    p.add_argument(
        "--benchmark-seconds",
        type=float,
        default=60.0,
        help="Stop a size after this many seconds and extrapolate (default 60).",
    )
    return p


def _apply(
    ctx: wsjrdp2027.WsjRdpContext, codes: list[str], read_codewords: list[str]
) -> int:
    pc = wsjrdp2027._payment_code
    rw_conn = ctx.hitobito_psycopg_connection(read_only=False)
    with rw_conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (GENERATION_LOCK_KEY,))
    now_codewords, _available = _read_codewords(rw_conn)
    known = set(read_codewords)
    appeared = [codeword for codeword in now_codewords if codeword not in known]
    if appeared:
        fresh = wsjrdp2027.IssuedCodewords(appeared)
        too_close = [
            code for code in codes if not keeps_distances(fresh, pc.codeword_of(code))
        ]
        if too_close:
            raise SystemExit(
                f"{len(appeared)} codes appeared while this run made its codes, "
                f"{len(too_close)} of the new ones come too close; nothing inserted, run again."
            )
        _LOGGER.info(
            "%d codes appeared in the meantime; none comes too close.", len(appeared)
        )
    placeholders = ", ".join(["%s"] * len(_COLUMNS))
    with rw_conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {_TABLE_NAME} ({', '.join(_COLUMNS)}) VALUES ({placeholders})",
            [row_for(code) for code in codes],
        )
    _LOGGER.info("Inserted %d available payment codes.", len(codes))
    if ctx.parsed_args.rollback_for_testing:
        _LOGGER.warning(
            "ROLLBACK (--rollback-for-testing given) - no changes committed"
        )
        rw_conn.rollback()
        return 0
    return len(codes)


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(
        argument_parser=create_argument_parser(),
        argv=argv,
        __file__=__file__,
    )
    args = ctx.parsed_args
    out_base = ctx.make_out_path(_SELF_NAME + "_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    if args.benchmark:
        sizes = [int(size) for size in args.benchmark_sizes.split(",") if size.strip()]
        run_benchmark(sizes, count=args.benchmark_count, seconds=args.benchmark_seconds)
        return 0

    pc = wsjrdp2027._payment_code
    with ctx:
        ro_conn = ctx.hitobito_psycopg_connection(read_only=True)
        codewords, available = _read_codewords(ro_conn)
        count = count_to_make(available, fill_to=args.fill_to, count=args.count)
        _LOGGER.info(
            "%d payment codes in the table, %d available; making %d.",
            len(codewords),
            available,
            count,
        )
        if count <= 0:
            _LOGGER.info("Nothing to make.")
            return 0

        start = _time.perf_counter()
        codes = make_codes(wsjrdp2027.IssuedCodewords(codewords), count)
        _LOGGER.info(
            "Made %d codes in %.1f s, e.g. %s",
            len(codes),
            _time.perf_counter() - start,
            ", ".join(pc.format_payment_code(code) for code in codes[:3]),
        )

        if ctx.dry_run:
            _LOGGER.info("[dry-run] Not inserting.")
            return 0

        ctx.require_approval_to_run_in_prod(
            prompt=f"Insert {len(codes)} available payment codes into {_TABLE_NAME}?"
        )
        inserted = _apply(ctx, codes, codewords)
        stock = available + inserted
        if stock < LOW_STOCK:
            _LOGGER.warning(
                "Only %d available payment codes (low stock below %d).",
                stock,
                LOW_STOCK,
            )
    return 0


if __name__ == "__main__":
    _sys.exit(main())
