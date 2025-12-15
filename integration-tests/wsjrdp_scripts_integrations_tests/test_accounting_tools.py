from __future__ import annotations

import json
import pathlib
import pathlib as _pathlib

import pytest_wsjrdp2027
from pytest_wsjrdp2027 import parse_sdd_xml


_SELFDIR = _pathlib.Path(__file__).parent.resolve()
_ROOT_DIR = (_SELFDIR / ".." / "..").resolve()


class Test_Run_Accounting_Tools:
    def test__accounting_tools__create_and_send_pre_notifications(
        self, run_wsjrdp_script_out_dir: pathlib.Path
    ):
        pytest_wsjrdp2027.uv_run(
            [
                f"{_ROOT_DIR}/accounting_tools/create_and_send_pre_notifications.py",
                "--skip-email",
                "--collection-date=2027-05-31",
                "--no-zip-eml",
                """--query=---
where:
  id: [4, 141, 182, 203, 204, 352, 356, 1000, 1189, 1395, 1422, 2145, 2147, 2437]
email_only_where:
  id: [140, 482, 484, 485, 486, 2246]
include_sepa_mail_in_mailing_to: true
""",
            ]
        )
        xml_files = list(run_wsjrdp_script_out_dir.rglob("*.xml"))
        assert len(xml_files) == 1
        sdd_xml_path = xml_files[0]
        sdd = parse_sdd_xml(sdd_xml_path)
        ctrl_sum_cents = sdd["ctrl_sum_cents"]

        json_files = list(run_wsjrdp_script_out_dir.rglob("*.json"))
        assert len(json_files) == 1
        with open(json_files[0], "r") as f:
            json_d = json.load(f)
        results = json_d["results"]
        sum_open_amount_cents = results["sum_open_amount_cents"]

        assert ctrl_sum_cents == sum_open_amount_cents

    def test__acounting_tools__2025_08_05__Mailing_SEPA_Delay(self, run_wsjrdp_script):
        run_wsjrdp_script("accounting_tools/2025-08-05--Mailing-SEPA-Delay.py")

    def test__acounting_tools__2025_08_10__Mailing_SEPA_Pre_Notification(
        self, run_wsjrdp_script
    ):
        run_wsjrdp_script(
            "accounting_tools/2025-08-10--Mailing-SEPA-Pre-Notification.py"
        )

    def test__acounting_tools__2025_11_07__SEPA_Delay(self, run_wsjrdp_script):
        run_wsjrdp_script("accounting_tools/2025-11-07--SEPA-Delay.py")
