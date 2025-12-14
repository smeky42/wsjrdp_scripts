from __future__ import annotations

import dataclasses as _dataclasses
import typing as _typing

from . import _util


if _typing.TYPE_CHECKING:
    import pandas as _pandas


PERSON_VERSION_COLS = [
    "additional_contact_adress_a",
    "additional_contact_adress_b",
    "additional_contact_email_a",
    "additional_contact_email_b",
    "additional_contact_name_a",
    "additional_contact_name_b",
    "additional_contact_phone_a",
    "additional_contact_phone_b",
    "additional_contact_single",
    "additional_information",
    "address_care_of",
    "birthday",
    "buddy_id",
    "buddy_id_ul",
    "buddy_id_yp",
    "can_swim",
    "cluster_code",
    "company",
    "company_name",
    "complete_document_upload_at",
    "contract_upload_at",
    "country",
    "diet",
    "early_payer",
    "email",
    "first_name",
    "foto_permission",
    "gender",
    "generated_registration_pdf",
    "housenumber",
    "language",
    "languages_spoken",
    "last_name",
    "latitude",
    "longitude",
    "medical_abnormalities",
    "medical_additional_vaccinations",
    "medical_allergies",
    "medical_continuous_medication",
    "medical_eating_disorders",
    "medical_infectious_diseases",
    "medical_medical_treatment_contact",
    "medical_mental_health",
    "medical_mobility_needs",
    "medical_needs_medication",
    "medical_other",
    "medical_person_of_trust",
    "medical_preexisting_conditions",
    "medical_self_treatment_medication",
    "medical_situational_support",
    "medical_stiko_vaccinations",
    "nickname",
    "passport_approved",
    "passport_germany",
    "passport_nationality",
    "payment_role",
    "postbox",
    "primary_group_id",
    "print_at",
    "pronoun",
    "rdp_association",
    "rdp_association_group",
    "rdp_association_number",
    "rdp_association_region",
    "rdp_association_sub_region",
    "sepa_address",
    "sepa_bic",
    "sepa_iban",
    "sepa_mail",
    "sepa_name",
    "sepa_status",
    "shirt_size",
    "status",
    "street",
    "town",
    "uniform_size",
    "unit_code",
    "upload_contract_pdf",
    "upload_data_agreement_pdf",
    "upload_good_conduct_pdf",
    "upload_medical_pdf",
    "upload_passport_pdf",
    "upload_photo_permission_pdf",
    "upload_recommendation_pdf",
    "upload_sepa_pdf",
    "zip_code",
]


@_dataclasses.dataclass(kw_only=True)
class _ScalarChange:
    old_col: str | None = None
    new_col: str
    col_type: type | None = None
    render_jinja2: bool = False

    @property
    def col_name(self) -> str:
        return self.new_col

    def __normalize(self, value):
        from . import _util

        if self.col_type and not issubclass(self.col_type, (int, float)):
            return _util.nan_to_none(value)
        else:
            return value

    def compute_df_val(self, row: _pandas.Series, new_val):
        if row.get("skip_db_updates") and self.old_col:
            return self.__normalize(row[self.old_col])
        elif self.render_jinja2:
            return _util.render_template(new_val, {"row": row})
        else:
            return new_val

    def get_old_val(self, row: _pandas.Series):
        if self.old_col:
            return self.__normalize(row[self.old_col])
        else:
            return None

    def get_new_val(self, row: _pandas.Series):
        return self.__normalize(row[self.new_col])


@_dataclasses.dataclass(kw_only=True)
class _StrListChange:
    old_col: str | None = None
    add_col: str

    @property
    def col_name(self) -> str:
        return self.add_col

    def compute_df_val(self, row: _pandas.Series, new_val):
        from . import _util

        if row.get("skip_db_updates") and self.old_col:
            return row[self.old_col]
        else:
            return _util.dedup(
                _util.to_str_list(row.get(self.add_col)) + _util.to_str_list(new_val)
            )

    def get_old_val(self, row: _pandas.Series):
        from . import _util

        if self.old_col:
            return _util.to_str_list(row[self.old_col])
        else:
            return []

    def get_new_val(self, row: _pandas.Series):
        from . import _util

        old_val = self.get_old_val(row)
        return _util.dedup(old_val + _util.to_str_list(row[self.add_col]))


PERSON_CHANGES: list[_ScalarChange | _StrListChange] = [
    *(_ScalarChange(old_col=col, new_col=f"new_{col}") for col in PERSON_VERSION_COLS),
    _ScalarChange(
        old_col="primary_group_role_types", new_col="new_primary_group_role_types"
    ),
    _ScalarChange(old_col=None, new_col="new_note", render_jinja2=True),
    _StrListChange(old_col="tag_list", add_col="add_tags"),
]

UPDATE_KEY_TO_CHANGE = {chg.col_name: chg for chg in PERSON_CHANGES}

VALID_PERSON_UPDATE_KEYS = frozenset(UPDATE_KEY_TO_CHANGE.keys())
