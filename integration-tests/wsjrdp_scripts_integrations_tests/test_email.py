from __future__ import annotations

import wsjrdp2027


def test_mail_login(ctx: wsjrdp2027.WsjRdpContext):
    mail_client = ctx.mail_login()
    assert mail_client
    with mail_client:
        pass
