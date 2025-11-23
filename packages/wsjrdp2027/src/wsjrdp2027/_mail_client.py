from __future__ import annotations

import collections.abc as _collections_abc
import contextlib as _contextlib
import datetime as _datetime
import logging as _logging
import re as _re
import typing as _typing


if _typing.TYPE_CHECKING:
    import datetime as _datetime
    import email.message as _email_message
    import email.policy as _email_policy
    import imaplib as _imaplib
    import smtplib as _smtplib

    from . import _mail_config, _mailing


_LOGGER = _logging.getLogger(__name__)

_ConfirmCallback = _collections_abc.Callable[
    ["_mail_config.WsjRdpMailConfig", "_email_message.EmailMessage"], bool
]


class MailClient:
    _config: _mail_config.WsjRdpMailConfig
    _dry_run: bool = False
    _asked_for_sending_in_prod: bool = False
    _confirm_send_callback: _ConfirmCallback | None = None
    _confirm_count: int = 0

    _logger: _logging.Logger | _logging.LoggerAdapter = _LOGGER
    _exit_stack: _contextlib.ExitStack
    _smtp: _smtplib.SMTP | None = None
    _imap: _imaplib.IMAP4 | None = None
    _imap_has_sent_mailbox: bool | None = None
    _imap_sent_mailbox: NameDelim | None = None

    def __init__(
        self,
        *,
        config: _mail_config.WsjRdpMailConfig,
        dry_run: bool | None = None,
        confirm_send_callback: _ConfirmCallback | None = None,
    ) -> None:
        from . import _util

        self._logger = _util.PrefixLoggerAdapter(_LOGGER, prefix="[MAIL]")
        self._config = config
        if dry_run is not None:
            self._dry_run = dry_run
        if confirm_send_callback is not None:
            self._confirm_send_callback = confirm_send_callback
        self._confirm_count = 0
        self._exit_stack = _contextlib.ExitStack()

    @property
    def config(self) -> _mail_config.WsjRdpMailConfig:
        return self._config

    def __smtp_connect(self, exit_stack: _contextlib.ExitStack | None) -> _smtplib.SMTP:
        import smtplib

        self._logger.info(
            "Connect to SMTP server %s:%s",
            self._config.smtp_server,
            self._config.smtp_port,
        )

        smtp = smtplib.SMTP(self._config.smtp_server, self._config.smtp_port)

        smtp.ehlo()

        has_starttls = smtp.has_extn("STARTTLS")
        self._logger.info("has SMTP STARTTLS? %s", has_starttls)

        if has_starttls:
            self._logger.debug("try SMTP STARTTLS")
            try:
                smtp.starttls()
            except Exception as exc:
                self._logger.error("SMTP STARTTLS failed: %s", str(exc))
                raise

        if self._config.smtp_username and self._config.smtp_password:
            self._logger.info("SMTP login as %s", self._config.smtp_username)
            smtp.login(self._config.smtp_username, self._config.smtp_password)
        else:
            self._logger.info("Skip SMTP login (credentials empty)")

        if exit_stack:

            def smtp_quit():
                self._logger.info("SMTP QUIT")
                smtp.quit()

            exit_stack.callback(smtp_quit)
        return smtp

    def __imap_connect(
        self, exit_stack: _contextlib.ExitStack | None
    ) -> _imaplib.IMAP4:
        import imaplib

        is_ssl = self._config.imap_ssl or (self._config.imap_port == 993)
        imap_cls = imaplib.IMAP4_SSL if is_ssl else imaplib.IMAP4
        imap = imap_cls(self._config.imap_server, self._config.imap_port)
        # imap.debug = 4
        imap.login(self._config.imap_username, self._config.imap_password)
        # self._logger.info("capabilities: %s", imap.capabilities)
        # self._logger.info("cap: %s", imap.capability())
        if exit_stack:
            exit_stack.enter_context(imap)
        return imap

    def __enter__(self):
        self._exit_stack.__enter__()
        if self._dry_run:
            self._logger.info("dry run: No actual connection to SMTP/IMAP servers")
            self._imap = None
            self._smtp = None
        else:
            self._smtp = self.__smtp_connect(self._exit_stack)
            self._logger.info("config.has_imap: %s", self._config.has_imap)
            if self._config.has_imap:
                self._logger.info("IMAP connect")
                self._imap = self.__imap_connect(self._exit_stack)
        return self

    def __exit__(self, *args):
        self._smtp = None
        self._imap = None
        self._imap_has_sent_mailbox = None
        self._imap_sent_mailbox = None
        self._exit_stack.__exit__(*args)

    def get_default_email_policy(self) -> _email_policy.EmailPolicy:
        import email.policy

        policy = email.policy.SMTP.clone(
            raise_on_defect=True, cte_type="7bit", verify_generated_headers=True
        )
        return policy  # type: ignore

    def send_message(
        self,
        msg: _email_message.EmailMessage | _mailing.PreparedEmailMessage,
        *,
        from_addr: str | None = None,
    ) -> tuple:
        """Send *msg* over SMTP and stores it in the Sent mailbox.

        Args:
          msg: The message to send.
          from_addr: The *from_addr* to be used for sending over
            SMTP. If not set, the envelope from address is extracted
            from *msg*.
        """
        import copy
        import email.utils

        from . import _mailing

        if self._smtp is None and not self._dry_run:
            err_msg = "Cannot send message: Not connected"
            self._logger.error(err_msg)
            raise RuntimeError(err_msg)
        if isinstance(msg, _mailing.PreparedEmailMessage):
            email_msg = msg.message
            email_msg_bytes = msg.eml
        else:
            email_msg = msg
            email_msg_bytes = msg.as_bytes()
        if "Date" in email_msg:
            email_date = email.utils.parsedate_to_datetime(email_msg["Date"])
            self._logger.debug("Use existing header 'Date: %s'", email_msg["Date"])
        else:
            email_date = _datetime.datetime.now().astimezone()
            email_msg = copy.copy(email_msg)
            email_msg["Date"] = email.utils.format_datetime(email_date)
            self._logger.debug("Set header 'Date: %s'", email_msg["Date"])
        if not from_addr and self._config.from_addr:
            self._logger.debug("Use from_addr=%s (from config)", self._config.from_addr)
            from_addr = self._config.from_addr

        if self._dry_run:
            self._logger.info("dry run: Skip email sending")
            return None, None
        elif not self._smtp:
            err_msg = "Not connected to SMTP server"
            self._logger.error(err_msg)
            raise RuntimeError(err_msg)

        if self._confirm_send_callback and self._confirm_count < 1:
            if not self._confirm_send_callback(self._config, email_msg):
                self._logger.info("Skip email sending - not confirmed")
            else:
                self._confirm_count += 1
        self._logger.debug("SMTP send_message(..., from_addr=%r)", from_addr)
        smtp_result = self._smtp.send_message(email_msg, from_addr=from_addr)
        self._logger.debug("  -> %s", str(smtp_result))
        if self._imap:
            if sent_mailbox := self.get_imap_sent_mailbox():
                self._logger.debug(
                    "Append message to IMAP mailbox %r (user: %s)",
                    sent_mailbox.name,
                    self._config.imap_username,
                )
                imap_result = _imap_append(
                    self._imap,
                    mailbox=sent_mailbox,
                    message=email_msg_bytes,
                    flags=r"\Seen",
                    date_time=email_date,
                    logger=self._logger,
                )
            else:
                self._logger.debug("No Sent mailbox => message is not stored")
                imap_result = None
        else:
            self._logger.debug("No IMAP client => message is not stored")
            imap_result = None
        return smtp_result, imap_result

    def get_imap_sent_mailbox(self) -> NameDelim | None:
        """Return name and delimiter for the Sent mailbox."""
        if self._imap_has_sent_mailbox:
            return self._imap_sent_mailbox
        else:
            sent_mailbox = self.find_first_imap_mailbox_with_flag(r"\Sent")
            if sent_mailbox is None:
                self._logger.warning("Could not find sent folder")
            self._imap_has_sent_mailbox = True
            self._imap_sent_mailbox = sent_mailbox
            return sent_mailbox

    def find_first_imap_mailbox_with_flag(self, flag: str) -> NameDelim | None:
        """Return name and delimiter of first mailbox having *flag*.

        Fetches a list of all mailboxed from the IMAP server (using
        the ``LIST`` command) and iterates over them in order,
        returning the first mailbox where the set of flags contains
        *flag*. Returns `None` if no such mailbox can be found.
        """
        if not (imap := self._imap):
            err_msg = "Not connected to IMAP server"
            self._logger.error(err_msg)
            raise RuntimeError(err_msg)
        self._logger.debug("find_first_imap_mailbox_with_flag(%r)", flag)
        list_out = imap.list()
        if list_out[0] != "OK":
            raise RuntimeError(f"IMAP4 LIST failed, output: {list_out!r}")
        for item in list_out[1]:
            if item is None:
                continue
            self._logger.debug("  check %s", str(item))
            name_delim, flags = _parse_imap_list_item(item)
            if flag in flags:
                self._logger.debug("  -> found %s", name_delim)
                return name_delim
        else:
            self._logger.debug("  -> None (no mailbox with flag %r found)", flag)
            return None


_NEWLINE_RE = _re.compile(rb"\r\n|\r|\n")


def _imap_append(
    imap: _imaplib.IMAP4,
    *,
    mailbox: str | NameDelim,
    message: bytes,
    flags: str | _collections_abc.Iterable[str] | None = None,
    date_time: _datetime.datetime | _datetime.date | int | float | str | None = None,
    logger: _logging.Logger | _logging.LoggerAdapter | None = None,
) -> tuple[str, list[bytes]]:
    import imaplib

    from . import _util

    if logger is None:
        logger = _LOGGER
    if isinstance(mailbox, tuple):
        mailbox = mailbox[0]
    if flags is None:
        imap_flags = _typing.cast(str, None)
    elif isinstance(flags, str):
        if flags.startswith("(") and flags.endswith(")"):
            imap_flags = flags
        else:
            imap_flags = f"({flags})"
    else:
        imap_flags = "(" + " ".join(flags) + ")"
    imap_date_time = imaplib.Time2Internaldate(_util.to_datetime(date_time))
    message = _NEWLINE_RE.sub(b"\r\n", message)
    append_args = [
        mailbox,
        *filter(None, [imap_flags, imap_date_time]),
        f"{{{len(message)}}}",
    ]
    logger.debug("APPEND %s", " ".join(append_args))
    ret = imap.append(mailbox, imap_flags, imap_date_time, message)
    logger.debug("  -> %s", str(ret))
    return ret  # type: ignore


# ==============================================================================
# IMAP4
# ==============================================================================

_AMPERSAND_ORD = ord("&")
_HYPHEN_ORD = ord("-")


class NameDelim(_typing.NamedTuple):
    name: str
    delim: str


def _modified_unbase64(value: bytearray) -> str:
    import binascii

    return binascii.a2b_base64(value.replace(b",", b"/") + b"===").decode("utf-16be")


def _utf7_decode(value: bytes) -> str:
    res = []
    encoded_chars = bytearray()
    for char in value:
        if char == _AMPERSAND_ORD and not encoded_chars:
            encoded_chars.append(_AMPERSAND_ORD)
        elif char == _HYPHEN_ORD and encoded_chars:
            if len(encoded_chars) == 1:
                res.append("&")
            else:
                res.append(_modified_unbase64(encoded_chars[1:]))
            encoded_chars = bytearray()
        elif encoded_chars:
            encoded_chars.append(char)
        else:
            res.append(chr(char))
    if encoded_chars:
        res.append(_modified_unbase64(encoded_chars[1:]))
    return "".join(res)


def _parse_imap_list_item(
    item: bytes | tuple[bytes, bytes],
) -> tuple[NameDelim, tuple[str, ...]]:
    import re

    item_re = re.compile(r"\((?P<flags>[\S ]*?)\) (?P<delim>[\S]+) (?P<name>.+)")
    if isinstance(item, bytes):
        m = re.search(item_re, _utf7_decode(item))
        if not m:
            raise ValueError(f"Failed to parse IMAP4 LIST output {item!r}")
        item_dict = m.groupdict()
        name = item_dict["name"]
        if name.startswith('"') and name.endswith('"'):
            name = name[1:-1]
    else:
        m = re.search(item_re, _utf7_decode(item[0]))
        if not m:
            raise ValueError(f"Failed to parse IMAP4 LIST output {item!r}")
        item_dict = m.groupdict()
        name = _utf7_decode(item[1])
    name = name.replace('\\"', '"')
    delim = item_dict["delim"].replace('"', "")
    flags = tuple(item_dict["flags"].split())
    return (NameDelim(name, delim), flags)
