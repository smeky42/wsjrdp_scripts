class Test_Run_Accounting_Tools:
    def test__accounting_tools__create_and_send_pre_notifications(
        self, run_wsjrdp_script
    ):
        run_wsjrdp_script(
            "accounting_tools/create_and_send_pre_notifications.py", "--no-accounting"
        )
