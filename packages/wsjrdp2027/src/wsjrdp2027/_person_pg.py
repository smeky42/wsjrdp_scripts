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


@_dataclasses.dataclass(kw_only=True, frozen=True, eq=True)
class _ScalarChange:
    old_col: str | None = None
    new_col: str
    col_type: type | None = None
    render_jinja2: bool = False

    @property
    def col_name(self) -> str:
        return self.new_col

    @property
    def col_names(self) -> list[str]:
        return [self.new_col]

    def __normalize(self, value):
        from . import _util

        if self.col_type and not issubclass(self.col_type, (int, float)):
            return _util.nan_to_none(value)
        else:
            return value

    def compute_df_val(
        self, row: _pandas.Series, column: str, value: _typing.Any, updates: dict
    ) -> _typing.Any:
        """Compute value for *row* in column *column* for user provided *value*."""
        if self.render_jinja2:
            return _util.render_template(value or "", {"row": row})
        else:
            return value

    def get_old_val(self, row: _pandas.Series):
        if self.old_col:
            return self.__normalize(row[self.old_col])
        else:
            return None

    def get_new_val(self, row: _pandas.Series):
        return self.__normalize(row[self.new_col])


@_dataclasses.dataclass(kw_only=True, frozen=True, eq=True)
class _StrListChange:
    old_col: str | None = None
    add_col: str
    remove_col: str | None = None

    @property
    def col_name(self) -> str:
        return self.add_col

    @property
    def col_names(self) -> list[str]:
        return list(filter(None, [self.add_col, self.remove_col]))

    def compute_df_val(
        self, row: _pandas.Series, column: str, value: _typing.Any, updates: dict
    ) -> _typing.Any:
        """Compute value for *row* in column *column* for user provided *value*."""
        from . import _util

        tag_list = sorted(set(_util.to_str_list(value)))

        # If we compute the dataframe value for `add_col`, we can and
        # should remove entries in `remove_col`
        if column == self.add_col:
            remove_tag_set = set(_util.to_str_list(updates.get(self.remove_col)))
            tag_list = [tag for tag in tag_list if tag not in remove_tag_set]

        return tag_list

    def get_old_val(self, row: _pandas.Series):
        from . import _util

        if self.old_col:
            return sorted(_util.to_str_list(row[self.old_col]))
        else:
            return []

    def get_new_val(self, row: _pandas.Series):
        from . import _util

        remove_tag_set = set(_util.to_str_list(row.get(self.remove_col)))

        old_tag_set = set(_util.to_str_list(self.get_old_val(row)))
        add_tag_set = set(_util.to_str_list(row.get(self.add_col)))
        return sorted(
            tag for tag in (old_tag_set | add_tag_set) if tag not in remove_tag_set
        )


PERSON_CHANGES: list[_ScalarChange | _StrListChange] = [
    *(_ScalarChange(old_col=col, new_col=f"new_{col}") for col in PERSON_VERSION_COLS),
    _ScalarChange(
        old_col="primary_group_role_types", new_col="new_primary_group_role_types"
    ),
    _ScalarChange(old_col=None, new_col="add_note", render_jinja2=True),
    _StrListChange(old_col="tag_list", add_col="add_tags", remove_col="remove_tags"),
]

UPDATE_KEY_TO_CHANGE = {
    col_name: chg for chg in PERSON_CHANGES for col_name in chg.col_names
}

VALID_PERSON_UPDATE_KEYS = frozenset(UPDATE_KEY_TO_CHANGE.keys())
