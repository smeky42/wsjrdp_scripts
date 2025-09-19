from __future__ import annotations

from ._context import (
    WsjRdpContext as WsjRdpContext,
    WsjRdpContextConfig as WsjRdpContextConfig,
)
from ._payment import (
    DB_PEOPLE_ALL_SEPA_STATUS as DB_PEOPLE_ALL_SEPA_STATUS,
    DB_PEOPLE_ALL_STATUS as DB_PEOPLE_ALL_STATUS,
    PaymentRole as PaymentRole,
    load_accounting_balance_in_cent as load_accounting_balance_in_cent,
    load_payment_dataframe as load_payment_dataframe,
    mandate_id_from_hitobito_id as mandate_id_from_hitobito_id,
    write_payment_dataframe_to_db as write_payment_dataframe_to_db,
    write_payment_dataframe_to_html as write_payment_dataframe_to_html,
    write_payment_dataframe_to_xlsx as write_payment_dataframe_to_xlsx,
)
from ._people import (
    load_people_dataframe as load_people_dataframe,
    write_people_dataframe_to_xlsx as write_people_dataframe_to_xlsx,
)
from ._sepa_direct_debit import (
    WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG as WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG,
    WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG as WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG,
    SepaDirectDebit as SepaDirectDebit,
    SepaDirectDebitConfig as SepaDirectDebitConfig,
    SepaDirectDebitPayment as SepaDirectDebitPayment,
    write_accounting_dataframe_to_sepa_dd as write_accounting_dataframe_to_sepa_dd,
)
from ._util import (
    console_confirm as console_confirm,
    create_dir as create_dir,
)


__all__ = [
    "DB_PEOPLE_ALL_SEPA_STATUS",
    "DB_PEOPLE_ALL_STATUS",
    "EMAIL_SIGNATURE_CMT",
    "EMAIL_SIGNATURE_HOC",
    "EMAIL_SIGNATURE_ORG",
    "WSJRDP_PAXBANK_ROVERWAY_DIRECT_DEBIT_CONFIG",
    "WSJRDP_SKATBANK_DIRECT_DEBIT_CONFIG",
    #
    "PaymentRole",
    "SepaDirectDebit",
    "SepaDirectDebitConfig",
    "SepaDirectDebitPayment",
    "WsjRdpContext",
    "WsjRdpContextConfig",
    "console_confirm",
    "create_dir",
    "load_accounting_balance_in_cent",
    "load_payment_dataframe",
    "load_people_dataframe",
    "mandate_id_from_hitobito_id",
    "write_accounting_dataframe_to_sepa_dd",
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
