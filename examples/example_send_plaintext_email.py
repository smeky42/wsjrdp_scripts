#!/usr/bin/env -S uv run
"""Example: Sending a plaintext email message."""

from __future__ import annotations

import email.message
import sys

import wsjrdp2027


def main(argv=None):
    ctx = wsjrdp2027.WsjRdpContext(argv=argv, log_level="DEBUG")

    with ctx.mail_login() as client:
        msg = email.message.EmailMessage()
        msg.set_content("Test-Nachricht\nSecond Line.\n\n-- \nSignature")
        msg["Subject"] = "Test-Nachricht Subject"
        msg["From"] = "anmeldung@worldscoutjamboree.de"
        msg["To"] = "example@example.com"

        print()
        print("Example email message:")
        print("=" * 80)
        print(msg.as_string())
        print("=" * 80)
        print()

        client.send_message(msg)


if __name__ == "__main__":
    sys.exit(main())
