import dataclasses
import datetime
import functools
import typing
import warnings

import registration_mapper


@dataclasses.dataclass
class RegistrationPerson:
    """Class for row data from registration person query."""

    COLUMN_NAMES: typing.ClassVar[list[str]]
    """Names of the db query columns."""

    # The fields in this dataclass must be named like the column names
    # in the DB query returned by `get_db_query()`.

    id: int
    role_wish: str | None
    passport_nationality: str
    first_name: str
    last_name: str
    nickname: str
    gender: str
    birthday: datetime.date
    email: str
    address: str
    town: str
    country: str
    zip_code: str
    primary_group_id: int
    additional_contact_name_a: str
    additional_contact_name_b: str
    additional_contact_single: bool
    generated_registration_pdf: str
    status: str

    def __post_init__(self) -> None:
        """Fix some data after __init__(), before object is being used."""
        # Strip all str-value fields and convert None -> "" if the
        # field type is `str`.
        for field in dataclasses.fields(self.__class__):
            value = getattr(self, field.name)
            if isinstance(value, str):
                new_value = value.strip()
                setattr(self, field.name, new_value)
                # if new_value != value:
                #     print(f"{self.id}: {field.name}: {value!r} -> {new_value!r}")
            elif field.type == str and value is None:
                setattr(self, field.name, "")
                # print(f"{self.id}: {field.name}: {value!r} -> {''!r}")
        # Convert additional_contact_single to bool
        self.additional_contact_single = bool(self.additional_contact_single)

    @classmethod
    def get_db_query(cls, where_clause: str) -> str:
        """SQL query to select columns according to the fields in this dataclass."""
        select_part = f"select {', '.join(cls.COLUMN_NAMES)}"
        where_part = f" where {where_clause}" if where_clause else ""
        return f"{select_part} from people {where_part};"

    @functools.cached_property
    def is_participant(self) -> bool:
        return registration_mapper.is_participant(self.role_wish)

    @property
    def additional_contact_names(self) -> list[str]:
        """List of additional contact names.

        Returned list contains only non-empty names.
        """
        return [
            name
            for name in [self.additional_contact_name_a, self.additional_contact_name_b]
            if name
        ]

    @property
    def name_on_id_card(self) -> str | None:
        """The name on the ID card."""
        return registration_mapper.name(self.first_name, self.nickname)

    @functools.cached_property
    def generated_registration_date(self) -> datetime.date | None:
        """Date when the registration PDF was generated."""
        return registration_mapper.get_date_from_generated_file_name(
            self.generated_registration_pdf
        )

    @property
    def name_of_legal_guardian(self) -> str:
        names = self.additional_contact_names
        if not self.is_participant or not names:
            return ""
        if self.additional_contact_single:
            return names[0]
        else:
            return " AND ".join(names)

    @property
    def relationship_of_legal_guardian_with_the_participant(self) -> str | None:
        """Return "4" if we have name_of_legal_guardian, None otherwise.

        Valid values:

        1 - Father
        2 - Mother
        3 - Grandparent
        4 - Other

        As we don't know the actual relationship, we return "4" (Other).
        """
        if self.name_of_legal_guardian:
            return "4"
        else:
            return None

    @property
    def date_of_guardian_consent(self) -> datetime.date | None:
        """Return the date when the registration PDF was generated if we have name_of_legal_guardian."""
        if self.name_of_legal_guardian:
            return self.generated_registration_date
        else:
            return None

    @functools.cached_property
    def k_reg_nationality(self) -> str | None:
        """Korean WSJ registration system country code for the nationality of the participant."""
        try:
            return registration_mapper.nationality(self.passport_nationality)
        except ValueError as exc:
            warnings.warn(
                f"{str(exc)}: passport_nationality={self.passport_nationality!r}"
            )
            return "-"

    @functools.cached_property
    def k_reg_nationality_city_of_residence(self) -> str | None:
        """Korean WSJ registartion system country code for the residence of the participant."""
        try:
            return registration_mapper.nationality(self.country)
        except ValueError as exc:
            warnings.warn(f"{str(exc)}: country={self.country!r}")
            return "-"


RegistrationPerson.COLUMN_NAMES = [
    field.name for field in dataclasses.fields(RegistrationPerson)
]
