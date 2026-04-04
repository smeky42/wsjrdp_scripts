from __future__ import annotations

import pytest
from wsjrdp2027._context import WsjRdpContext


class Test_Context_ContextManager:
    @pytest.fixture
    def ctx(self, wsjrdp_context):
        return wsjrdp_context

    def test___del__(self, wsjrdp_config, caplog):
        ctx = WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)
        del ctx
        captured_log = caplog.text
        assert "Finished context cleanup (level=0)" in captured_log
