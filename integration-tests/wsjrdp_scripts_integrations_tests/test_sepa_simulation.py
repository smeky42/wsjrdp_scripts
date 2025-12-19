from __future__ import annotations

import contextlib as _contextlib
import datetime as _datetime
import json
import logging as _logging
import pathlib as _pathlib

import pytest
import pytest_wsjrdp2027
import wsjrdp2027


_DAY_OF_MONTH = 5
_FIRST_COLLECTION_DATE = _datetime.date(2025, 12, _DAY_OF_MONTH)
_CUSTOM_PRE_NOTIFICATION_QUERY = """---
where:
  id: [4, 141, 203, 204]
"""
# _CUSTOM_PRE_NOTIFICATION_QUERY = None


_LOGGER = _logging.getLogger(__name__)
_SELFDIR = _pathlib.Path(__file__).parent.resolve()
_ROOT_DIR = (_SELFDIR / ".." / "..").resolve()


@pytest.fixture(scope="module", autouse=True)
def sepa_simulation_fixture():
    pytest_wsjrdp2027.restore_integration_tests_db()
    yield


def _load_output_json_from_dir(path: _pathlib.Path, /) -> dict:
    json_files = list(path.rglob("*.json"))
    assert len(json_files) == 1
    with open(json_files[0], "r") as f:
        json_d = json.load(f)
    return json_d


def _load_sdd_xml_summary_from_glob(
    path: _pathlib.Path, pattern="*.xml"
) -> dict | None:
    xml_files = list(path.rglob(pattern))
    if len(xml_files) == 0:
        return None
    assert len(xml_files) == 1
    sdd_xml_path = xml_files[0]
    sdd = pytest_wsjrdp2027.parse_sdd_xml(sdd_xml_path)
    return sdd


class Test_Run_Monthly_SEPA_Direct_Debits:
    def test_simulate_SEPA_until_june_2027(
        self,
        ctx: wsjrdp2027.WsjRdpContext,
        run_wsjrdp_script_out_dir: _pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
    ):
        @_contextlib.contextmanager
        def change_out_dir(name):
            out_dir = run_wsjrdp_script_out_dir / name
            out_dir.mkdir(exist_ok=True, parents=True)
            with monkeypatch.context() as m:
                m.setenv("WSJRDP_SCRIPTS_OUTPUT_DIR__OVERRIDE", str(out_dir))
                m.chdir(out_dir)
                yield out_dir

        with change_out_dir("setup") as p:
            pytest_wsjrdp2027.uv_run(
                [
                    f"{_ROOT_DIR}/tools/mailing_from_yml.py",
                    str(_SELFDIR / "_confirm_reviewed.yml"),
                ]
            )

        today = _FIRST_COLLECTION_DATE

        today2results = {}

        while today < _datetime.date(2027, 7, 1):
            _LOGGER.info("today: %s", str(today))

            with change_out_dir(str(today)) as p:
                pre_notification_cmd = [
                    f"{_ROOT_DIR}/accounting_tools/create_and_send_pre_notifications.py",
                    "--skip-email",
                    f"--collection-date={today}",
                ]
                if _CUSTOM_PRE_NOTIFICATION_QUERY is not None:
                    pre_notification_cmd.append(
                        f"--query={_CUSTOM_PRE_NOTIFICATION_QUERY}"
                    )
                pytest_wsjrdp2027.uv_run(pre_notification_cmd)
                json_d = _load_output_json_from_dir(p)
                results = json_d["results"]
                pain_id = results["payment_initiation_id"]
                today2results[today] = results
                sum_open_amount_cents = results["sum_open_amount_cents"]
                _LOGGER.info(
                    "             sum_open_amount_cents = %s  /  %s",
                    sum_open_amount_cents,
                    wsjrdp2027.format_cents_as_eur_de(sum_open_amount_cents),
                )
                if (
                    pn := _load_sdd_xml_summary_from_glob(p, "pre_notification*.xml")
                ) is not None:
                    pn_ctrl_sum_cents = pn["ctrl_sum_cents"]
                    _LOGGER.info(
                        "   pre_notification ctrl_sum_cents = %s  /  %s",
                        pn_ctrl_sum_cents,
                        wsjrdp2027.format_cents_as_eur_de(pn_ctrl_sum_cents),
                    )
                else:
                    _LOGGER.info("   pre_notification ctrl_sum_cents = N/A")
                pytest_wsjrdp2027.uv_run(
                    [
                        f"{_ROOT_DIR}/accounting_tools/sepa_direct_debit.py",
                        f"--collection-date={today}",
                        f"--payment-initiation-id={pain_id}",
                    ]
                )
                if (
                    dd := _load_sdd_xml_summary_from_glob(p, "sepa_direct_debit*.xml")
                ) is not None:
                    dd_ctrl_sum_cents = dd["ctrl_sum_cents"]
                    _LOGGER.info(
                        "  sepa_direct_debit ctrl_sum_cents = %s  /  %s",
                        dd_ctrl_sum_cents,
                        wsjrdp2027.format_cents_as_eur_de(dd_ctrl_sum_cents),
                    )
                else:
                    _LOGGER.info("  sepa_direct_debit ctrl_sum_cents = N/A")

            today = (today + _datetime.timedelta(days=32)).replace(day=_DAY_OF_MONTH)

        _LOGGER.info("")
        sum_of_sum_open_amount_cents = 0
        for d, results in today2results.items():
            sum_open_amount_cents = results["sum_open_amount_cents"]
            _LOGGER.info(
                "%s  :: sum_open_amount: %s",
                str(d),
                wsjrdp2027.format_cents_as_eur_de(sum_open_amount_cents),
            )
            sum_of_sum_open_amount_cents += sum_open_amount_cents
        _LOGGER.info("")
        _LOGGER.info(
            "sum of sum_open_amount: %s",
            wsjrdp2027.format_cents_as_eur_de(sum_of_sum_open_amount_cents),
        )
        _LOGGER.info("")
