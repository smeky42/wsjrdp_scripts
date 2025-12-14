from __future__ import annotations


class Test_Run_Example:
    def test__examples__example_send_plaintext_email(self, run_wsjrdp_script):
        run_wsjrdp_script("examples/example_send_plaintext_email.py")

    def test__examples__example_people_dataframe(self, run_wsjrdp_script):
        run_wsjrdp_script("examples/example_people_dataframe.py")

    def test__examples__example_sepa_direct_debit(self, run_wsjrdp_script):
        run_wsjrdp_script("examples/example_sepa_direct_debit.py")
