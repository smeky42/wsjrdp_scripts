from __future__ import annotations

import typing as _typing

from ._batch import (
    BatchConfig as BatchConfig,
    PreparedBatch as PreparedBatch,
    PreparedEmailMessage as PreparedEmailMessage,
)
from ._context import (
    WsjRdpContext as WsjRdpContext,
    WsjRdpContextConfig as WsjRdpContextConfig,
)
from ._mail_client import MailClient as MailClient
from ._payment import (
    DB_PEOPLE_ALL_SEPA_STATUS as DB_PEOPLE_ALL_SEPA_STATUS,
    DB_PEOPLE_ALL_STATUS as DB_PEOPLE_ALL_STATUS,
    insert_direct_debit_pre_notification_from_row as insert_direct_debit_pre_notification_from_row,
    load_accounting_balance_in_cent as load_accounting_balance_in_cent,
    load_payment_dataframe as load_payment_dataframe,
    load_payment_dataframe_from_payment_initiation as load_payment_dataframe_from_payment_initiation,
    write_payment_dataframe_to_db as write_payment_dataframe_to_db,
    write_payment_dataframe_to_html as write_payment_dataframe_to_html,
    write_payment_dataframe_to_xlsx as write_payment_dataframe_to_xlsx,
)
from ._payment_role import PaymentRole as PaymentRole
from ._people import (
    load_people_dataframe as load_people_dataframe,
    write_people_dataframe_to_xlsx as write_people_dataframe_to_xlsx,
)
from ._people_query import PeopleQuery as PeopleQuery, PeopleWhere as PeopleWhere
from ._pg import (
    pg_add_person_tag as pg_add_person_tag,
    pg_insert_camt_transaction_from_tx as pg_insert_camt_transaction_from_tx,
    pg_insert_direct_debit_payment_info as pg_insert_direct_debit_payment_info,
    pg_insert_direct_debit_pre_notification as pg_insert_direct_debit_pre_notification,
    pg_insert_fin_account as pg_insert_fin_account,
    pg_insert_payment_initiation as pg_insert_payment_initiation,
)
from ._sepa_direct_debit import (
    CREDITOR_ID as CREDITOR_ID,
    WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG as WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG,
    WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG as WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG,
    SepaDirectDebit as SepaDirectDebit,
    SepaDirectDebitPayment as SepaDirectDebitPayment,
    write_accounting_dataframe_to_sepa_dd as write_accounting_dataframe_to_sepa_dd,
)
from ._types import (
    SepaDirectDebitConfig as SepaDirectDebitConfig,
)
from ._typst import (
    get_typst_font_paths as get_typst_font_paths,
    typst_compile as typst_compile,
)
from ._util import (
    configure_file_logging as configure_file_logging,
    console_confirm as console_confirm,
    create_dir as create_dir,
    format_cents_as_eur_de as format_cents_as_eur_de,
    format_iban as format_iban,
    get_default_email_policy as get_default_email_policy,
    merge_mail_addresses as merge_mail_addresses,
    nan_to_none as nan_to_none,
    render_template as render_template,
    sepa_mandate_id_from_hitobito_id as sepa_mandate_id_from_hitobito_id,
    to_date as to_date,
    to_date_or_none as to_date_or_none,
    to_datetime as to_datetime,
    to_datetime_or_none as to_datetime_or_none,
    to_int_or_none as to_int_or_none,
    to_str_list as to_str_list,
    to_yaml_str as to_yaml_str,
    write_dataframe_to_xlsx as write_dataframe_to_xlsx,
)


if _typing.TYPE_CHECKING:
    from ._camt import CamtMessage as CamtMessage


__all__ = [
    "CREDITOR_ID",
    "DB_PEOPLE_ALL_SEPA_STATUS",
    "DB_PEOPLE_ALL_STATUS",
    "DEFAULT_MSGID_DOMAIN",
    "DEFAULT_MSGID_IDSTRING",
    "EMAIL_SIGNATURE_CMT",
    "EMAIL_SIGNATURE_HOC",
    "EMAIL_SIGNATURE_ORG",
    "EMAIL_SIGNATURE_DEBIT_PRE_NOTIFICATION",
    "WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG",
    "WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG",
    #
    "BatchConfig",
    "CamtMessage",
    "MailClient",
    "PaymentRole",
    "PeopleQuery",
    "PeopleWhere",
    "PreparedBatch",
    "PreparedEmailMessage",
    "SepaDirectDebit",
    "SepaDirectDebitConfig",
    "SepaDirectDebitPayment",
    "WsjRdpContext",
    "WsjRdpContextConfig",
    "configure_file_logging",
    "console_confirm",
    "create_dir",
    "format_cents_as_eur_de",
    "format_iban",
    "get_default_email_policy",
    "get_typst_font_paths",
    "insert_direct_debit_pre_notification_from_row",
    "load_accounting_balance_in_cent",
    "load_payment_dataframe",
    "load_payment_dataframe_from_payment_initiation",
    "load_people_dataframe",
    "merge_mail_addresses",
    "nan_to_none",
    "pg_add_person_tag",
    "pg_insert_camt_transaction_from_tx",
    "pg_insert_direct_debit_payment_info",
    "pg_insert_direct_debit_pre_notification",
    "pg_insert_fin_account",
    "pg_insert_payment_initiation",
    "render_template",
    "sepa_mandate_id_from_hitobito_id",
    "to_date",
    "to_date_or_none",
    "to_datetime",
    "to_datetime_or_none",
    "to_int_or_none",
    "to_str_list",
    "to_yaml_str",
    "typst_compile",
    "write_accounting_dataframe_to_sepa_dd",
    "write_dataframe_to_xlsx",
    "write_payment_dataframe_to_db",
    "write_payment_dataframe_to_html",
    "write_payment_dataframe_to_xlsx",
    "write_people_dataframe_to_xlsx",
]

EMAIL_SIGNATURE_CMT = (
    "\n-- "
    + """
World Scout Jamboree 2027 Poland
Contingent Management Team

Ring deutscher Pfadfinder*innenverbände e.V. (rdp)
Chausseestr. 128/129
10115 Berlin

info@worldscoutjamboree.de
https://worldscoutjamboree.de"""
)


EMAIL_SIGNATURE_HOC = (
    "\n-- "
    + """
World Scout Jamboree 2027 Poland
Head of Contingent

Ring deutscher Pfadfinder*innenverbände e.V. (rdp)
Chausseestr. 128/129
10115 Berlin

info@worldscoutjamboree.de
https://worldscoutjamboree.de"""
)


EMAIL_SIGNATURE_ORG = (
    "\n-- "
    + """
World Scout Jamboree 2027 Poland
Head of Organisation

Ring deutscher Pfadfinder*innenverbände e.V. (rdp)
Chausseestr. 128/129
10115 Berlin

info@worldscoutjamboree.de
https://worldscoutjamboree.de
"""
)

EMAIL_SIGNATURE_DAVID_FRITZSCHE = (
    "\n-- "
    + """
World Scout Jamboree 2027 Poland
German Contingent
Head of Finance Team

Ring deutscher Pfadfinder*innenverbände e.V. (rdp)
Chausseestr. 128/129
10115 Berlin
"""
)

EMAIL_SIGNATURE_DEBIT_PRE_NOTIFICATION = (
    "\n-- "
    + """
Falls du Fragen hast, schau auf unserer Homepage https://worldscoutjamboree.de vorbei oder wende dich an info@worldscoutjamboree.de.

German Contingent | Kontyngent Niemiecki
World Scout Jamboree 2027 Poland

Ring deutscher Pfadfinder*innenverbände e.V. (rdp)
Chausseestr. 128/129
10115 Berlin
"""
)

DEFAULT_MSGID_DOMAIN = "worldscoutjamboree.de"
DEFAULT_MSGID_IDSTRING = "wsjrsp2027"


EARLY_PAYER_AUGUST_IDS_SUPERSET = [
    14, 16, 59, 65, 82, 141, 144, 147, 148, 149, 150, 156, 158, 159, 160, 161, 166, 167,
    170, 172, 176, 188, 190, 192, 198, 200, 204, 206, 207, 208, 214, 217, 219, 220, 231,
    235, 241, 251, 252, 253, 263, 264, 269, 271, 275, 287, 288, 292, 295, 297, 298, 300,
    302, 308, 310, 311, 315, 317, 326, 329, 355, 357, 358, 371, 374, 384, 389, 392, 399,
    402, 409, 414, 415, 417, 428, 430, 433, 442, 443, 444, 447, 448, 451, 455, 456, 463,
    466, 471, 498, 503, 508, 518, 519, 526, 527, 528, 532, 533, 534, 535, 539, 545, 546,
    547, 555, 556, 566, 567, 568, 571, 572, 585, 586, 587, 588, 594, 596, 597, 601, 603,
    606, 608, 610, 615, 619, 620, 623, 625, 631, 632, 640, 642, 643, 645, 646, 671, 673,
    675, 678, 681, 682, 684, 685, 691, 692, 702, 703, 712, 721, 723, 725, 726, 728, 732,
    737, 738, 745, 747, 748, 751, 755, 759, 760, 761, 765, 779, 782, 783, 784, 785, 787,
    788, 789, 799, 807, 808, 812, 813, 819, 820, 822, 829, 832, 854, 864, 865, 868, 870,
    878, 879, 886, 891, 895, 908, 998, 999, 1002, 1007, 1011, 1012, 1013, 1014, 1015,
    1025, 1029, 1036, 1039, 1050, 1055, 1068, 1094, 1097, 1126, 1128, 1129, 1132, 1133,
    1142, 1153
]  # fmt: skip
"""Super-set of the Hitobito id's of the August 2025 Early Payers.

This set was fixed before sending the Pre-Notification.
"""


__ALIASES__ = {
    "CamtMessage": (f"._camt", "CamtMessage"),
}


def __getattr__(name):
    import importlib

    mod_name, qualname = __ALIASES__.get(name, (None, None))
    if not mod_name or not qualname:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    mod = importlib.import_module(mod_name, package=__name__)
    obj = getattr(mod, qualname)
    globals()[qualname] = obj
    return obj
