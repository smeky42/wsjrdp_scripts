from __future__ import annotations


class Test_Run_Tools:
    def test__tools__stats_roles_status(self, run_wsjrdp_script):
        run_wsjrdp_script("tools/stats_roles_status.py")

    # def test__tools__update_longtitude_latitude(self, run_wsjrdp_script):
    #     run_wsjrdp_wsjrdp_script("tools/update_longtitude_latitude.py")

    def test__tools__move_longtitude_latitude(self, run_wsjrdp_script):
        run_wsjrdp_script("tools/move_longtitude_latitude.py")
