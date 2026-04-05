from __future__ import annotations

import gc
import logging
import weakref

import pytest
from wsjrdp2027._context import WsjRdpContext


class Test_Context_ContextManager:
    @pytest.fixture
    def ctx(self, wsjrdp_context):
        return wsjrdp_context

    def test___del__(self, wsjrdp_config, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        ctx = WsjRdpContext(wsjrdp_config, parse_arguments=False, setup_logging=False)
        ctx_ref = weakref.ref(ctx)
        del ctx
        gc.collect()
        assert ctx_ref() is None
        captured_log = caplog.text
        assert "Finished context cleanup (level=0)" in captured_log
