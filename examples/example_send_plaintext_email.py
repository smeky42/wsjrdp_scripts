#!/usr/bin/env -S uv run
"""Example: Sending a plaintext email message."""

from __future__ import annotations

import sys
from email.message import EmailMessage

import wsjrdp2027


def main():
    ctx = wsjrdp2027.ConnectionContext(log_level="DEBUG")

    with ctx.smtp_login() as client:
        msg = EmailMessage()
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

        prompt = f"Do you want to send the message via SMTP server {ctx.config.smtp_server}:{ctx.config.smtp_port}"
        if wsjrdp2027.console_confirm(prompt, default=False):
            print("Sending message...")
            client.send_message(msg)
        else:
            print("Not sending message")


if __name__ == "__main__":
    sys.exit(main())
