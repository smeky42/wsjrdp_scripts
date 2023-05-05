#!/usr/bin/env python
import os
import sys
import typing
import warnings
from datetime import date

import yaml
from mysql.connector import connection
from openpyxl import load_workbook

import registration_mapper
from registration_person import RegistrationPerson


def to_sheet_row_dict(p: RegistrationPerson, no: int) -> dict[str, typing.Any]:
    d: dict[str, typing.Any] = {}
    # fmt: off
    d["A"] = str(no)  # No.
    d["B"] = p.korea_id # Korea ID
    # d["B"] = registration_mapper.type(p.role_wish)  # "Type - (Youth participant, Adult participant)" X
    # d["C"] = "57"  # Name of NSO X
    # d["D"] = registration_mapper.position(p.role_wish)  # Position X 
    # d["E"] = p.k_reg_nationality  # Nationality X
    # d["F"] = "-"  # Hangeul
    # d["G"] = "-"  # Roman alphabet
    # d["H"] = p.last_name  # Surname X
    # d["I"] = "-"  # Middle Name
    # d["J"] = p.first_name  # Given Name X
    d["C"] = p.name_on_id_card  # Name on ID card X
    # d["L"] = registration_mapper.gender(p.gender)  # Gender X
    # d["M"] = p.k_birthday  # Date of birth X
    d["D"] = p.email  # Participant's email
    # d["O"] = "1"  # Your affiliation(Scouting) X
    # d["P"] = "-"  # Job/position X
    # d["Q"] = "-"  # Current position within the NSO
    d["E"] = p.address or "-" # Home address
    d["F"] = p.town or "-" # City
    d["G"] = "-"  # State/Province
    d["H"] = p.k_reg_nationality_city_of_residence  # Nationality(City)
    d["I"] = p.zip_code or "-" # Zip code
    d["J"] = "-"  # Home phone number
    d["K"] = "-"  # "Mobile phone number - (Country code)"
    d["L"] = "-"  # "Mobile phone number - (phone number)"
    d["M"] = "7"  # "SNS ID - (social media account)"
    d["N"] = p.hitobito_link  # SNS URL
    d["O"] = p.name_of_legal_guardian  # Name of legal guardian
    d["P"] = "-"  # Phone number of legal guardian
    d["Q"] = p.adress_of_legal_guardian  # Email address of legal guardian
    d["R"] = "-"  # Primart emergency contact name
    d["S"] = "-"  # "Relationship with primary  - emergency contact"
    d["T"] = "-"  # "Primary emergency  - contact phone number"
    d["U"] = "-"  # "secondart emergency  - contact name"
    d["V"] = "-"  # Relationship with secondary emergency contact
    d["W"] = "-"  # Secondary emergency contact phone number
    d["X"] = p.passport_number or "-"  # Passport number
    d["Y"] = "-"  # Date of issue
    d["Z"] = p.passport_valid or "-" # Valid until
    d["AA"] = p.k_reg_nationality  # Passport issuing country
    d["AB"] = "-"  # Means of transportation
    d["AC"] = "-"  # Ariline
    d["AD"] = "-"  # Date of departure
    d["AE"] = "-"  # Arrival airport
    d["AF"] = "-"  # Date of arrival
    d["AG"] = "-"  # Time of arrival
    d["AH"] = "-"  # Flight number
    d["AI"] = "-"  # Origin point / Last city of boarding
    d["AJ"] = "-"  # Date of departure
    d["AK"] = "-"  # departure time
    d["AL"] = "-"  # Blood type
    d["AM"] = "-"  # Blood type - Other
    d["AN"] = "-"  # Underlying health conditions
    d["AO"] = "-"  # Underlying health conditions - Other
    d["AP"] = "-"  # History of surgery or hospitalization
    d["AQ"] = "-"  # Name of medication
    d["AR"] = "-"  # Dosage
    d["AS"] = "-"  # Frequency
    d["AT"] = "-"  # Reason for medication intake
    d["AU"] = p.k_allergies  # Allergies
    d["AV"] = p.k_allergies_other  # Allergies � Other
    d["AW"] = "-"  # Allergies � specific details
    d["AX"] = p.k_food_allergies  # Food allergies
    d["AY"] = p.k_food_allergies_other  # Food allergies - Other
    d["AZ"] = "-"  # "Types of COVID-19 vaccines �  - first dose"
    d["BA"] = "-"  # "Types of COVID-19 vaccines �  - Second dose"
    d["BB"] = "-"  # "Types of COVID-19 vaccines �  - Third dose"
    d["BC"] = "-"  # "Types of COVID-19 vaccines �  - Fourth dose"
    d["BD"] = "-"  # Dates vaccinated � First dose
    d["BE"] = "-"  # Dates vaccinated � Seconf dose
    d["BF"] = "-"  # Dates vaccinated � Third dose
    d["BG"] = "-"  # Dates vaccinated � Fourth dose
    d["BH"] = "-"  # Tetanus
    d["BI"] = "-"  # Hepatitis A
    d["BJ"] = "-"  # Pertussis
    d["BK"] = "-"  # Hepatitis B
    d["BL"] = "-"  # Diphtheria
    d["BM"] = "-"  # Encephalomeningitis
    d["BN"] = "-"  # Measles/Mumps/Rubella
    d["BO"] = "-"  # Influenza
    d["BP"] = "-"  # Polio
    d["BQ"] = "-"  # Chickenpox
    d["BR"] = "For information on medication or health status contact the german contingent medical team on an individual level. "  # Other
    d["BS"] = p.shirt_size or "-" # Shirt Size
    d["BT"] = p.k_dietary_needs  # Dietary needs
    d["BU"] = p.k_dietary_needs_other  # Dietary needs - Other
    d["BV"] = p.k_mobility_needs  # The mobility aids that are being brought
    d["BW"] = p.k_mobility_needs_other  # Mobility needs - Other
    d["BX"] = "-"  # Special needs
    d["BY"] = "-"  # Religion
    d["BZ"] = "-"  # Religion - Other
    d["CA"] = "-"  # Languages spoken
    d["CB"] = "-"  # Languages spoken � Other
    d["CC"] = "-"  # "Langauges spoken  - (advanced, intermediate, beginner)"
    d["CD"] = "-"  # Insurance
    d["CE"] = "-"  # Name of insurance company
    d["CF"] = "-"  # Phone number of insurance company
    d["CG"] = "-"  # Insurance certificate
    d["CH"] = "-"  # Prior experience of participating in a WSJ (World Scout Jamboree)
    d["CI"] = "-"  # Prior experience of participating in a WSJ - Other
    d["CJ"] = "-"  # Past WSJ role(s)
    d["CK"] = "N"  # Participation in the Pre-Jamboree Activities
    d["CL"] = "-"  # Boarding the official Jamboree shuttle bus
    d["CM"] = "-"  # Preferred time to arrive at the Jamboree site(Date)
    d["CN"] = "-"  # Preferred time to arrive at the Jamboree site(Time)
    d["CO"] = "-"  # Participation in the Post-Jamboree Activities
    d["CP"] = "-"  # Boarding the official Jamboree shuttle bus
    d["CQ"] = "-"  # Preferred departure time from Jamboree site(Date)
    d["CR"] = "-"  # Preferred departure time from Jamboree site(Time)
    d["CS"] = p.name_of_legal_guardian or "-" # Name of legal guardian
    d["CT"] = p.relationship_of_legal_guardian_with_the_participant or "-" # Relationship of legal guardian with the participant
    d["CU"] = p.date_of_guardian_consent or "-" # Date of parental/guardian consent
    # fmt: on
    return d


def main():
    with open("../config.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read successful")

    today = str(date.today())
    cnx = connection.MySQLConnection(
        user=config["username"],
        password=config["password"],
        host="anmeldung.worldscoutjamboree.de",
        port=config["port"],
        database=config["database"],
    )

    # where_clause = "role_wish = 'Teilnehmende*r' limit 200"
    # where_clause = ""and id not in (2, 2432, 2428, 2413, 2386, 2375, 2360, 1912, 1810, 626, 625, 312)""
    # where_clause = "id=2"
    # where_clause = "id > 2 and (status = 'bestätigt durch KT' or status = 'bestätigt durch Leitung' or status = 'vollständig')"
    where_clause = ("id > 1 "
                    "and role_wish <> '' "
                    "and status not in ('abgemeldet', 'Abmeldung Vermerkt', 'in Überprüfung durch KT', '')"
                    "limit 1")

    cursor = cnx.cursor(dictionary=True)
    cursor.execute(RegistrationPerson.get_db_query(where_clause))

    print("Read database")

    # load excel file
    workbook = load_workbook(filename="wsj_update_en.xlsx")

    # open workbook
    sheet: typing.Any = workbook.active

    counter = 0
    for counter, row_dict in enumerate(cursor, start=1):
        p = RegistrationPerson(**row_dict)

        # Catch all warnings generated while collecting the data for
        # the next row in the sheet.
        with warnings.catch_warnings(record=True) as warnings_list:
            sheet_row_dict = to_sheet_row_dict(p, no=counter)

        # If we got some warnings, print them
        if warnings_list:
            print(f"Error(s): id={p.id} {p.first_name} {p.last_name}")
            for warning_item in warnings_list:
                print(f"  - {warning_item.message}")

        # write row data into sheet
        row = str(counter + 4)
        for col, val in sheet_row_dict.items():
            if val is not None:
                sheet[col + row] = val

    cursor.close()

    # save the file
    # os.makedirs("upload_korea", exist_ok=True)
    workbook.save(filename=today + "--wsj_update_de.xlsx")
    print(f"Wrote {counter} rows")


if __name__ == "__main__":
    sys.exit(main())
