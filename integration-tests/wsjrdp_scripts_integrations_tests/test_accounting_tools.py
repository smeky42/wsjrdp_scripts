from __future__ import annotations

import pathlib


class Test_Run_Accounting_Tools:
    def test__accounting_tools__create_and_send_pre_notifications(
        self, run_wsjrdp_script, run_wsjrdp_script_out_dir: pathlib.Path
    ):
        from .wsjrdp_script_runner import parse_sdd_xml

        run_wsjrdp_script(
            "accounting_tools/create_and_send_pre_notifications.py",
            "--no-accounting",
            "--collection-date=2027-05-31",
        )
        xml_files = list(run_wsjrdp_script_out_dir.rglob("*.xml"))
        assert len(xml_files) == 1
        sdd_xml_path = xml_files[0]
        sdd = parse_sdd_xml(sdd_xml_path)
        assert sdd["ctrl_sum_cents"] > 100_000_00

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
