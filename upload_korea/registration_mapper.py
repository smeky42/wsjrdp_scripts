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
            return "I"
        case None:
            warnings.warn("Keine Rolle: role_wish=None")
            return "-"
        case _:
            warnings.warn(f"Ungültige Rolle: role_wish={role_wish!r}")
            return "-"


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

def dietary_needs(medicine_eating_disorders: str | None):
    if not medicine_eating_disorders or "keine" in medicine_eating_disorders.lower() or len(medicine_eating_disorders) < 3:
        return "1"
    elif "vegan" in medicine_eating_disorders.lower():
        return "2"
    elif "vege" in medicine_eating_disorders.lower():
        return "3"
    elif "kosher" in medicine_eating_disorders.lower():
        return "4"
    elif "halal" in medicine_eating_disorders.lower():
        return "5"
    else:
      return "6"
    
def allergies(medicine_allergies: str | None):
    if not medicine_allergies or "keine" in medicine_allergies.lower() or len(medicine_allergies) < 3:
        return "1"
    else:
      return "2|3|4|5"
  
def food_allergies(medicine_allergies: str | None, medicine_eating_disorders: str | None):
    if dietary_needs(medicine_eating_disorders) == "1" and allergies(medicine_allergies) == "1":
        return "1"
    
    food_allergies_str = medicine_eating_disorders.lower() + medicine_allergies.lower()
    food_allergies_map = "10"

    if "fisch" in food_allergies_str or "meer" in food_allergies_str: 
        food_allergies_map += "|2|4"

    if "lactose" in food_allergies_str: 
        food_allergies_map += "|3"
    
    if "gluten" in food_allergies_str: 
        food_allergies_map += "|5"
    
    if "weizen" in food_allergies_str or "getreide" in food_allergies_str: 
        food_allergies_map += "|7"
    
    if "früchte" in food_allergies_str: 
        food_allergies_map += "|8"
    
    if "eier" in food_allergies_str or "egg" in food_allergies_str: 
        food_allergies_map += "|2"

    return food_allergies_map

    
def mobility_needs(medicine_mobility_needs: str | None):
    if not medicine_mobility_needs or "keine" in medicine_mobility_needs.lower() or len(medicine_mobility_needs) < 5:
        return "1"
    elif "krücken" in medicine_mobility_needs.lower():
        return "2"
    elif "rollstuhl" in medicine_mobility_needs.lower():
        return "3"
    else:
        return "4"