# registration_mapper.py

import datetime
import re
import warnings


def is_participant(role_wish):
    return role_wish == "Teilnehmende*r"


def type(role_wish):
    match role_wish:
        case "Teilnehmende*r":
            return "1"
        case _:
            return "2"


def position(role_wish):
    match role_wish:
        case "Teilnehmende*r":
            return "S"
        case "Unit Leitung":
            return "L"
        case "Kontingentsteam":
            return "C"
        case "IST":
            return "S"
        case None:
            warnings.warn("Keine Rolle: role_wish=None")
            return " "
        case _:
            warnings.warn(f"Ungültige Rolle: role_wish={role_wish!r}")
            return " "


def name(first_name, nickname):
    name = first_name
    if nickname and len(nickname) > 2:
        name = nickname

    return name


def gender(gender):
    match gender:
        case "m":
            return "M"
        case "w":
            return "F"
        case _:
            return "O"


def nationality(passport_nationality):
    if not passport_nationality:
        raise ValueError("Keine Nationalität")
    if (
        "deu" in passport_nationality.lower()
        or passport_nationality == "D"
        or passport_nationality == "DE"
    ):
        return "49"
    elif (
        "Öster" in passport_nationality
        or "Oester" in passport_nationality
        or passport_nationality == "AT"
    ):
        return "43"
    elif "Belg" in passport_nationality:
        return "32"
    elif "Pol" in passport_nationality:
        return "48"
    elif "Franz" in passport_nationality or passport_nationality == "FR":
        return "33"
    elif "Finn" in passport_nationality:
        return "358"
    elif "Syr" in passport_nationality:
        return "963"
    elif "Südkorea" in passport_nationality:
        return "82"
    elif "Span" in passport_nationality:
        return "34"
    elif "Filip" in passport_nationality:
        return "63"
    elif "Türk" in passport_nationality:
        return "90"
    elif "Nied" in passport_nationality or passport_nationality == "NL":
        return "31"
    elif "Ungar" in passport_nationality:
        return "36"
    elif "Brit" in passport_nationality:
        return "44"
    elif "Bol" in passport_nationality:
        return "591"
    elif "Ital" in passport_nationality or passport_nationality == "IT":
        return "39"
    elif "Port" in passport_nationality:
        return "351"
    elif "Aserb" in passport_nationality:
        return "994"
    elif "Russ" in passport_nationality:
        return "7"
    elif "CH" == passport_nationality:
        return "41"
    elif "IE" == passport_nationality:
        return "353"
    elif "SE" == passport_nationality:
        return "100"
    else:
        raise ValueError("Konnte Nationalität nicht mappen")


def get_date_from_generated_file_name(filename: str | None) -> datetime.date | None:
    if not filename:
        return None
    basename = filename.split("/")[-1]
    if re.match("[0-9]{4}-[0-9]{2}-[0-9]{2}", basename):
        date_string = basename[:10]
        return datetime.date.fromisoformat(date_string)
    else:
        return None
