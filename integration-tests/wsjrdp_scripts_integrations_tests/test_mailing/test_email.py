from __future__ import annotations

import pathlib as _pathlib

import wsjrdp2027


_SELFDIR = _pathlib.Path(__file__).parent.resolve()


def test_mail_login(ctx: wsjrdp2027.WsjRdpContext):
    mail_client = ctx.mail_login()
    assert mail_client
    with mail_client:
        pass
