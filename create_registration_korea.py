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
    d["B"] = registration_mapper.type(p.role_wish)  # "Type - (Youth participant, Adult participant)" X
    d["C"] = "57"  # Name of NSO X
    d["D"] = registration_mapper.position(p.role_wish)  # Position X 
    d["E"] = p.k_reg_nationality  # Nationality X
    d["F"] = ""  # Hangeul
    d["G"] = ""  # Roman alphabet
    d["H"] = p.last_name  # Surname X
    d["I"] = ""  # Middle Name
    d["J"] = p.first_name  # Given Name X
    d["K"] = p.name_on_id_card  # Name on ID card X
    d["L"] = registration_mapper.gender(p.gender)  # Gender X
    d["M"] = p.birthday  # Date of birth X
    d["N"] = p.email  # Participant's email
    d["O"] = "1"  # Your affiliation(Scouting) X
    d["P"] = ""  # Job/position X
    d["Q"] = ""  # Current position within the NSO
    d["R"] = p.address  # Home address
    d["S"] = p.town  # City
    d["T"] = ""  # State/Province
    d["U"] = p.k_reg_nationality_city_of_residence  # Nationality(City)
    d["V"] = p.zip_code  # Zip code
    d["W"] = "-"  # Home phone number
    d["X"] = "-"  # "Mobile phone number - (Country code)"
    d["Y"] = "-"  # "Mobile phone number - (phone number)"
    d["Z"] = "7"  # "SNS ID - (social media account)"
    d["AA"] = p.hitobito_link  # SNS URL
    d["AB"] = p.name_of_legal_guardian  # Name of legal guardian
    d["AC"] = "-"  # Phone number of legal guardian
    d["AD"] = p.adress_of_legal_guardian  # Email address of legal guardian
    d["AE"] = "-"  # Primart emergency contact name
    d["AF"] = "-"  # "Relationship with primary  - emergency contact"
    d["AG"] = "-"  # "Primary emergency  - contact phone number"
    d["AH"] = "-"  # "secondart emergency  - contact name"
    d["AI"] = "-"  # Relationship with secondary emergency contact
    d["AJ"] = "-"  # Secondary emergency contact phone number
    d["AK"] = p.passport_number  # Passport number
    d["AL"] = ""  # Date of issue
    d["AM"] = p.passport_valid  # Valid until
    d["AN"] = p.k_reg_nationality  # Passport issuing country
    d["AO"] = "-"  # Means of transportation
    d["AP"] = "-"  # Ariline
    d["AQ"] = "-"  # Date of departure
    d["AR"] = "-"  # Arrival airport
    d["AS"] = "-"  # Date of arrival
    d["AT"] = "-"  # Time of arrival
    d["AU"] = "-"  # Flight number
    d["AV"] = "-"  # Origin point / Last city of boarding
    d["AW"] = "-"  # Date of departure
    d["AX"] = "-"  # departure time
    d["AY"] = "-"  # Blood type
    d["AZ"] = "-"  # Blood type - Other
    d["BA"] = "-"  # Underlying health conditions
    d["BB"] = "-"  # Underlying health conditions - Other
    d["BC"] = "-"  # History of surgery or hospitalization
    d["BD"] = "-"  # Name of medication
    d["BE"] = "-"  # Dosage
    d["BF"] = "-"  # Frequency
    d["BG"] = "-"  # Reason for medication intake
    d["BH"] = p.k_allergies  # Allergies
    d["BI"] = p.k_allergies_other  # Allergies � Other
    d["BJ"] = ""  # Allergies � specific details
    d["BK"] = p.k_food_allergies  # Food allergies
    d["BL"] = p.k_food_allergies_other  # Food allergies - Other
    d["BM"] = "-"  # "Types of COVID-19 vaccines �  - first dose"
    d["BN"] = "-"  # "Types of COVID-19 vaccines �  - Second dose"
    d["BO"] = "-"  # "Types of COVID-19 vaccines �  - Third dose"
    d["BP"] = "-"  # "Types of COVID-19 vaccines �  - Fourth dose"
    d["BQ"] = "-"  # Dates vaccinated � First dose
    d["BR"] = "-"  # Dates vaccinated � Seconf dose
    d["BS"] = "-"  # Dates vaccinated � Third dose
    d["BT"] = "-"  # Dates vaccinated � Fourth dose
    d["BU"] = "-"  # Tetanus
    d["BV"] = "-"  # Hepatitis A
    d["BW"] = "-"  # Pertussis
    d["BX"] = "-"  # Hepatitis B
    d["BY"] = "-"  # Diphtheria
    d["BZ"] = "-"  # Encephalomeningitis
    d["CA"] = "-"  # Measles/Mumps/Rubella
    d["CB"] = "-"  # Influenza
    d["CC"] = "-"  # Polio
    d["CD"] = "-"  # Chickenpox
    d["CE"] = "For information on medication or health status contact the german contingent medical team on an individual level. "  # Other
    d["CF"] = p.shirt_size  # Shirt Size
    d["CG"] = p.k_dietary_needs  # Dietary needs
    d["CH"] = p.k_dietary_needs_other  # Dietary needs - Other
    d["CI"] = p.k_mobility_needs  # The mobility aids that are being brought
    d["CJ"] = p.k_mobility_needs_other  # Mobility needs - Other
    d["CK"] = "-"  # Special needs
    d["CL"] = "-"  # Religion
    d["CM"] = "-"  # Religion - Other
    d["CN"] = "-"  # Languages spoken
    d["CO"] = "-"  # Languages spoken � Other
    d["CP"] = "-"  # "Langauges spoken  - (advanced, intermediate, beginner)"
    d["CQ"] = "-"  # Insurance
    d["CR"] = "-"  # Name of insurance company
    d["CS"] = "-"  # Phone number of insurance company
    d["CT"] = "-"  # Insurance certificate
    d["CU"] = "-"  # Prior experience of participating in a WSJ (World Scout Jamboree)
    d["CV"] = "-"  # Prior experience of participating in a WSJ - Other
    d["CW"] = "-"  # Past WSJ role(s)
    d["CX"] = "N"  # Participation in the Pre-Jamboree Activities
    d["CY"] = "-"  # Boarding the official Jamboree shuttle bus
    d["CZ"] = "-"  # Preferred time to arrive at the Jamboree site(Date)
    d["DA"] = "-"  # Preferred time to arrive at the Jamboree site(Time)
    d["DB"] = "-"  # Participation in the Post-Jamboree Activities
    d["DC"] = "-"  # Boarding the official Jamboree shuttle bus
    d["DD"] = "-"  # Preferred departure time from Jamboree site(Date)
    d["DE"] = "-"  # Preferred departure time from Jamboree site(Time)
    d["DF"] = p.name_of_legal_guardian  # Name of legal guardian
    d["DG"] = p.relationship_of_legal_guardian_with_the_participant  # Relationship of legal guardian with the participant
    d["DH"] = p.date_of_guardian_consent  # Date of parental/guardian consent
    # fmt: on
    return d


def main():
    with open("config.yml", "r") as yamlfile:
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
    # where_clause = ""
    # where_clause = "id=2"
    where_clause = "id > 2 and (status = 'bestätigt durch KT' or status = 'bestätigt durch Leitung' or status = 'vollständig')"

    cursor = cnx.cursor(dictionary=True)
    cursor.execute(RegistrationPerson.get_db_query(where_clause))

    # load excel file
    workbook = load_workbook(filename="wsj_insert_en.xlsx")

    # open workbook
    sheet: typing.Any = workbook.active

    counter = 0
    for counter, row_dict in enumerate(cursor, start=1):
        p = RegistrationPerson(**row_dict)

        # Catch all warnings generated while collecting the data for
        # the next row in the sheet.
        with warnings.catch_warnings(record=True) as warnings_list:
            sheet_row_dict = to_sheet_row_dict(p, no=counter + 12)

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
    os.makedirs("upload_korea", exist_ok=True)
    workbook.save(filename="upload_korea/" + today + "--wsj_insert_de.xlsx")
    print(f"Wrote {counter} rows")


if __name__ == "__main__":
    sys.exit(main())
