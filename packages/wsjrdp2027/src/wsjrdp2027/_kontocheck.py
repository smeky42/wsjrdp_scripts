from __future__ import annotations

import threading as _threading

_kontocheck_lut_loaded = False
_kontocheck_lock = _threading.Lock()


def _ensure_lut():
    global _kontocheck_lut_loaded
    global _kontocheck_lock

    import kontocheck

    if not _kontocheck_lut_loaded:
        with _kontocheck_lock:
            if not _kontocheck_lut_loaded:
                kontocheck.lut_load()
                _kontocheck_lut_loaded = True


def check_iban(iban: str) -> bool:
    import kontocheck

    _ensure_lut()
    return kontocheck.check_iban(iban)


def get_bic(iban: str) -> str:
    import kontocheck

    _ensure_lut()
    return kontocheck.get_bic(iban)


def is_bic_compatible(bic_a: str | None, bic_b: str | None) -> bool:
    if (
        (bic_a is None or bic_b is None)
        or (bic_a == bic_b)
        or (bic_a + "XXX" == bic_b)
        or (bic_a == bic_b + "XXX")
    ):
        return True
    else:
        return False
