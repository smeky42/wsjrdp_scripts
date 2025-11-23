from __future__ import annotations

import dataclasses as _dataclasses


@_dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class WsjRdpMailConfig:
    smtp_server: str = ""
    smtp_port: int = 0
    smtp_username: str = ""
    smtp_password: str = _dataclasses.field(default="", repr=False)

    imap_server: str = ""
    imap_port: int = 0
    imap_ssl: bool | None = None
    imap_username: str = ""
    imap_password: str = _dataclasses.field(default="", repr=False)

    email_from: str | None = None
    from_addr: str | None = None

    def __post_init__(self) -> None:
        if self.email_from and not self.from_addr:
            import email.utils

            from_addr = email.utils.parseaddr(self.email_from)[1]
            object.__setattr__(self, "from_addr", from_addr)

    @property
    def has_imap(self) -> bool:
        return bool(
            self.imap_server
            and self.imap_port
            and self.imap_username
            and self.imap_password
        )
