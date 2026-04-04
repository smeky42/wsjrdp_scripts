from __future__ import annotations

import typing as _typing


if _typing.TYPE_CHECKING:
    import collections.abc as _collections_abc

    from . import _context, _person


def ensure_moss_email_mailbox_or_alias(
    ctx: _context.WsjRdpContext, *, people: _collections_abc.Iterable[_person.Person]
) -> None:
    from . import mailbox
    from ._util import console_confirm

    all_mail_aliases = mailbox.get_aliases(ctx)
    addr2goto = {
        a["address"]: frozenset(a["goto"].split(",")) for a in all_mail_aliases
    }

    for p in people:
        expected_goto = p.moss_email_expected_goto
        if expected_goto and (expected_goto != p.moss_email):
            print(p.unit_or_role, p.moss_email, "->", expected_goto)
            goto_addrs = set(s.lower() for s in addr2goto.get(p.moss_email, set()))
            if expected_goto.lower() not in goto_addrs:
                print(f"    ! Missing alias {p.moss_email} -> {expected_goto}")
                print(f"    {p.primary_group.name}  status: {p.status}")
                if console_confirm("Add missing mail alias?"):
                    mailbox.add_alias(ctx, p.moss_email, goto=expected_goto)


def moss_email_with_expected_goto(p: _person.Person, /) -> str:
    assert p.moss_email
    expected_goto = p.moss_email_expected_goto
    if expected_goto and (expected_goto != p.moss_email):
        return f"{p.moss_email} -> {expected_goto}"
    else:
        return p.moss_email
