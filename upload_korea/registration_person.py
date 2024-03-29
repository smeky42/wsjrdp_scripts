import dataclasses
import datetime
import functools
import typing
import warnings
from dateutil.relativedelta import relativedelta

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
    passport_number: str
    passport_nationality: str
    passport_valid: datetime.date
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
    medicine_allergies: str
    medicine_mobility_needs: str
    medicine_eating_disorders: str 
    additional_contact_name_a: str
    additional_contact_adress_a: str
    additional_contact_name_b: str
    additional_contact_adress_b: str
    additional_contact_single: bool
    generated_registration_pdf: str
    shirt_size: str
    korea_id: str
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
    
    @functools.cached_property
    def hitobito_link(self) -> str | None:
        """Link to hitobito profile on worldscoutjamboree.de"""
        return f"https://anmeldung.worldscoutjamboree.de/groups/{self.primary_group_id}/people/{self.id}.html"
    
    @property
    def additional_contact_name(self) -> list[str]:
        """List of additional contact names.

        Returned list contains only non-empty names.
        """
        return [
            name
            for name in [self.additional_contact_name_a, self.additional_contact_name_b]
            if name
        ]


    @property
    def additional_contact_adress(self) -> list[str]:
        """List of additional contact adress.

        Returned list contains only non-empty adress.
        """
        return [
            adress
            for adress in [self.additional_contact_adress_a, self.additional_contact_adress_b]
            if adress
        ]

    @property
    def name_on_id_card(self) -> str | None:
        """The name on the ID card."""
        return registration_mapper.name(self.first_name, self.nickname)


    @property
    def k_passport_number(self) -> str | None:
        """The name on the ID card."""
        return self.passport_number.replace(" ","")[:15]


    @functools.cached_property
    def generated_registration_date(self) -> datetime.date | None:
        """Date when the registration PDF was generated."""
        return registration_mapper.get_date_from_generated_file_name(
            self.generated_registration_pdf
        )

    @functools.cached_property
    def k_birthday(self) -> datetime.date | None:
        """Birthday fitting to Role"""

        if self.is_participant and self.birthday < datetime.date(2005,7,22):
          new_birthday = self.birthday + relativedelta(years=1)
          warnings.warn(f"age_of_partithipant={self.birthday} corrected to {new_birthday} should be younger than 2005-07-22")
          return new_birthday
        
        if not self.is_participant and self.birthday > datetime.date(2005,7,22):
          warnings.warn(f"age_of_partithipant={self.birthday} should be older than 2005-07-22")

         
        return self.birthday
    
    @functools.cached_property
    def k_shirt_size(self) -> str | None:
        """Return - if we are not able to map the size.

        Valid values:

        XS
        S
        M
        L
        XL
        2XL
        3XL
        4XL 
       
        """

        if self.shirt_size in [ "XS", "S", "M", "L", "XL", "2XL", "3XL", "4XL"]:
            return self.shirt_size

        # warnings.warn(f"shirt_size={self.shirt_size} not valid using -")
        
        return "-"



    @property
    def name_of_legal_guardian(self) -> str:
        names = self.additional_contact_name
        if not self.is_participant or not names:
            return "-"
        if self.additional_contact_single:
            return registration_mapper.replace_invalid(names[0])
        else:
            return registration_mapper.replace_invalid(" AND ".join(names))
        
    @property
    def adress_of_legal_guardian(self) -> str:
        adress = self.additional_contact_adress
        if not self.is_participant or not adress:
            return self.address 
        if self.additional_contact_single:
            return adress[0]
        else:
            return " AND ".join(adress)
    
    @property
    def email_adress_of_legal_guardian(self) -> str:
        return self.email


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

    @functools.cached_property
    def k_dietary_needs(self) -> str | None:
        """Korean WSJ registartion system dietary need codes 
        
        Valid Values:
        
        1 No dietary needs
        2 Vegan
        3 Vegetarian
        4 Kosher
        5 Halal
        6 Other

        If thre is a value in our System but we are not able to identify a need,  
        we return "6 Other".
        """
        try:
            return registration_mapper.dietary_needs(self.medicine_eating_disorders)
        except ValueError as exc:
            warnings.warn(f"{str(exc)}: country={self.country!r}")
            return "6"
    
    @functools.cached_property
    def k_dietary_needs_other(self) -> str | None:
        """Korean WSJ registration system dietary needs.
        
        As there is the possibility to have multiple dietary needs we return the needs
        no matter what is stated in k_dietary_needs exept it is 1"""
        try:
            if not self.k_dietary_needs == "1":
              return registration_mapper.replace_invalid(self.medicine_eating_disorders)
            else: 
             return "-"
        except ValueError as exc:
            warnings.warn(
                f"{str(exc)}: medicine_eating_disorders={self.medicine_eating_disorders!r}"
            )
            return "-"
        
    @functools.cached_property
    def k_allergies(self) -> str | None:
        """Korean WSJ registartion system allergie need codes 
        
        Valid Values:
        
        1 No allergy
        2 Drug
        3 Plants
        4 Insects
        5 Other

        If there is a value in our System we return "2|3|4|5" as we are not able to specify.
        """
        try:
            return registration_mapper.allergies(self.medicine_allergies)
        except ValueError as exc:
            warnings.warn(f"{str(exc)}: medicine_allergies={self.medicine_allergies!r}")
            return "-"
    
    @functools.cached_property
    def k_allergies_other(self) -> str | None:
        """Korean WSJ registration system dietary needs.
        
        As there is the possibility to have multiple dietary needs we return the needs
        no matter what is stated in k_dietary_needs exept it is 1"""
        try:
            if not self.k_allergies == "1":
              return registration_mapper.replace_invalid(self.medicine_allergies)
            else: 
             return ""
        except ValueError as exc:
            warnings.warn(
                f"{str(exc)}: medicine_allergies={self.medicine_allergies!r}"
            )
            return ""

    @functools.cached_property
    def k_food_allergies(self) -> str | None:
        """Korean WSJ registartion system allergie need codes 
        
        Valid Values:
        
        1 No food allergy
        2 Fish
        3 Lactose
        4 Seafood
        5 Gluten
        6 Nuts
        7 Wheat
        8 Fruits
        9 Egg
        10 Other

        If there is a value in our System but we are not able to identify a need,  
        we return "10 Other"
        """
        try:
            return registration_mapper.food_allergies(self.medicine_allergies, self.medicine_eating_disorders)
        except ValueError as exc:
            warnings.warn(f"{str(exc)}: medicine_allergies={self.medicine_allergies!r}  {self.medicine_eating_disorders!r}")
            return "-"
    
    @functools.cached_property
    def k_food_allergies_other(self) -> str | None:
        """Korean WSJ registration system food allergies.
        
        As there is the possibility to have multiple food allergies we return the needs
        no matter what is stated in k_food_allergies exept it is 1"""
        try:
            if not self.k_food_allergies == "1":
              return registration_mapper.replace_invalid(self.medicine_eating_disorders)
            else: 
             return ""
        except ValueError as exc:
            warnings.warn(
                f"{str(exc)}: medicine_allergies={self.medicine_allergies!r}  {self.medicine_eating_disorders!r}"
            )
            return ""




    @functools.cached_property
    def k_mobility_needs(self) -> str | None:
        """Korean WSJ registartion system mobility need codes 
        
        Valid Values:
        
        1 None applicable
        2 Cranes or Crutches
        3 Wheelchairs
        4 Other

        If thre is a value in our System but we are not able to identify a need,  
        we return "4 Other".
        """
        try:
            return registration_mapper.mobility_needs(self.medicine_mobility_needs)
        except ValueError as exc:
            warnings.warn(f"{str(exc)}: country={self.country!r}")
            return "4"
    
    @functools.cached_property
    def k_mobility_needs_other(self) -> str | None:
        """Korean WSJ registration mobiltity dietary needs.
        
        As there is the possibility to have multiple mobility needs we return the needs
        no matter what is stated in k_mobility_needs exept it is 1"""
        try:
            if not self.k_mobility_needs == "1":
              return registration_mapper.replace_invalid(self.medicine_mobility_needs)
            else: 
             return "-"
        except ValueError as exc:
            warnings.warn(
                f"{str(exc)}: medicine_mobility_needs={self.medicine_mobility_needs!r}"
            )
            return "-"
        
    @functools.cached_property
    def k_shuttle(self) -> str | None:
      """Korean WSJ registration shuttle departure from jamboree
         Echter Flug: C1 (17:55Uhr, EY857), C2 (17:55 Uhr, EY857) und C4 (12:35 Uhr, CI161)

         Transfer zu Nachtour: C3, C5, C6, C7, C8, D8, E5, D4, D5, E7 (im Zweifel 17:55 mit EY857)
          14: C1 Schwarze Teufelskralle - 40
          15: C2 Großer Hufeisenklee - 39
          16: C3 Zwerg Goldstern - 40
          17: C4 Hibiskus - 40
          18: C5 Sonnentau - 40
          19: C6 Schöner Blaustern - 40
          20: C7 Pyrenäen Drachenmaul - 35
          10: D4 Meeresleuchten - 38
          11: D5 Kornrade - 38
          54: D8 Schlangenäuglein - 27
          34: E5 Schachblume - 39
          36: E7 Waldmeister - 40
      """

      if self.primary_group_id in [14,15,16,17,18,19,20,10,11,54,34,36]:
          return "Y"

      return "N"
    
    @functools.cached_property
    def k_shuttle_date(self) -> str | None:
        """Korean WSJ registration shuttle departure date is 12.8.
           Scout 12.8. -> 1
           Unit Leader 12.8. -> 3
      """
        if self.k_shuttle == "Y": 
          if registration_mapper.position(self.role_wish) == "S":
              return "1"
          if registration_mapper.position(self.role_wish) == "L":
              return "3" 
        
        return "-"
    
    @functools.cached_property
    def k_shuttle_time(self) -> str | None:
        """Korean WSJ registration shuttle departure date is 12.8.
          C4 -> 17: 4:00-5:00 -> 5
          Other -> 10:00-11:00 -> 11
          
      """
        if self.k_shuttle == "Y":
          if self.primary_group_id == 17:
              return "5"
          
          return "11" 
        
        return "-"
    
    @functools.cached_property
    def k_date_of_departure(self) -> str | None:
        """Korean WSJ registration shuttle departure date is 12.8.
          C4 -> 17: 4:00-5:00 -> 5
          Other -> 10:00-11:00 -> 11
          
      """
        if self.k_shuttle == "Y":
          return "2023-12-08" 
        
        return "2023-12-31"


    @functools.cached_property
    def k_departure_time(self) -> str | None:
        """Korean WSJ registration shuttle departure date is 12.8.
          C4 -> 17: 4:00-5:00 -> 5
          Other -> 10:00-11:00 -> 11
          
      """
        if self.k_shuttle == "Y":
          if self.primary_group_id == 17:
              return "12"
          return "18" 
        
        return "1"

RegistrationPerson.COLUMN_NAMES = [
    field.name for field in dataclasses.fields(RegistrationPerson)
]
