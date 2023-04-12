
import os
from openpyxl import load_workbook
from mysql.connector import (connection)
from datetime import date
import yaml
import registration_mapper


with open("config.yml", "r") as yamlfile:
    config = yaml.load(yamlfile, Loader=yaml.FullLoader)
    print("Read successful")

today = str(date.today())
cnx = connection.MySQLConnection(user=config['username'], password=config['password'],
                                 host='anmeldung.worldscoutjamboree.de',
                                 port=config['port'],
                                 database=config['database'])
cursor = cnx.cursor()
query =   ("select role_wish, first_name, last_name, gender, primary_group_id, zip_code, status from people where id=2;")
cursor.execute(query)
#load excel file
workbook = load_workbook(filename="wsj_insert_en.xlsx")

#open workbook
sheet = workbook.active

counter = 5
for (role_wish, first_name, last_name, gender, primary_group_id, zip_code, status) in cursor:
    row = str(counter)
    sheet["A" + row] = str(counter + 7) # No.
    sheet["B" + row] = registration_mapper.type(role_wish) # "Type - (Youth participant, Adult participant)"
    sheet["C" + row] = "57" # Name of NSO
    sheet["D" + row] = registration_mapper.position(role_wish) # Position
    sheet["E" + row] = "-" # Nationality
    sheet["F" + row] = "-" # Hangeul
    sheet["G" + row] = "-" # Roman alphabet
    sheet["H" + row] = "-" # Surname
    sheet["I" + row] = "-" # Middle Name
    sheet["J" + row] = "-" # Given Name
    sheet["K" + row] = "-" # Name on ID card
    sheet["L" + row] = "-" # Gender
    sheet["M" + row] = "-" # Date of birth
    sheet["N" + row] = "-" # Participant's email
    sheet["O" + row] = "-" # Your affiliation(Scouting)
    sheet["P" + row] = "-" # Job/position
    sheet["Q" + row] = "-" # Current position within the NSO
    sheet["R" + row] = "-" # Home address
    sheet["S" + row] = "-" # City
    sheet["T" + row] = "-" # State/Province
    sheet["U" + row] = "-" # Nationality(City)
    sheet["V" + row] = "-" # Zip code
    sheet["W" + row] = "-" # Home phone number
    sheet["X" + row] = "-" # "Mobile phone number - (Country code)"
    sheet["Y" + row] = "-" # "Mobile phone number - (phone number)"
    sheet["Z" + row] = "-" # "SNS ID - (social media account)"
    sheet["AA" + row] = "-" # SNS URL
    sheet["AB" + row] = "-" # Name of legal guardian
    sheet["AC" + row] = "-" # Phone number of legal guardian
    sheet["AD" + row] = "-" # Email address of legal guardian
    sheet["AE" + row] = "-" # Primart emergency contact name
    sheet["AF" + row] = "-" # "Relationship with primary  - emergency contact"
    sheet["AG" + row] = "-" # "Primary emergency  - contact phone number"
    sheet["AH" + row] = "-" # "secondart emergency  - contact name"
    sheet["AI" + row] = "-" # Relationship with secondary emergency contact
    sheet["AJ" + row] = "-" # Secondary emergency contact phone number
    sheet["AK" + row] = "-" # Passport number
    sheet["AL" + row] = "-" # Date of issue
    sheet["AM" + row] = "-" # Valid until
    sheet["AN" + row] = "-" # Passport issuing country
    sheet["AO" + row] = "-" # Means of transportation
    sheet["AP" + row] = "-" # Ariline
    sheet["AQ" + row] = "-" # Date of departure
    sheet["AR" + row] = "-" # Arrival airport
    sheet["AS" + row] = "-" # Date of arrival
    sheet["AT" + row] = "-" # Time of arrival
    sheet["AU" + row] = "-" # Flight number
    sheet["AV" + row] = "-" # Origin point / Last city of boarding
    sheet["AW" + row] = "-" # Date of departure
    sheet["AX" + row] = "-" # departure time
    sheet["AY" + row] = "-" # Blood type
    sheet["AZ" + row] = "-" # Blood type - Other
    sheet["BA" + row] = "-" # Underlying health conditions
    sheet["BC" + row] = "-" # Underlying health conditions - Other
    sheet["BD" + row] = "-" # History of surgery or hospitalization
    sheet["BE" + row] = "-" # Name of medication
    sheet["BF" + row] = "-" # Dosage
    sheet["BG" + row] = "-" # Frequency
    sheet["BH" + row] = "-" # Reason for medication intake
    sheet["BI" + row] = "-" # Allergies
    sheet["BJ" + row] = "-" # Allergies � Other
    sheet["BK" + row] = "-" # Allergies � specific details
    sheet["BL" + row] = "-" # Food allergies
    sheet["BM" + row] = "-" # Food allergies - Other
    sheet["BN" + row] = "-" # "Types of COVID-19 vaccines �  - first dose"
    sheet["BO" + row] = "-" # "Types of COVID-19 vaccines �  - Second dose"
    sheet["BP" + row] = "-" # "Types of COVID-19 vaccines �  - Third dose"
    sheet["BQ" + row] = "-" # "Types of COVID-19 vaccines �  - Fourth dose"
    sheet["BR" + row] = "-" # Dates vaccinated � First dose
    sheet["BS" + row] = "-" # Dates vaccinated � Seconf dose
    sheet["BT" + row] = "-" # Dates vaccinated � Third dose
    sheet["BU" + row] = "-" # Dates vaccinated � Fourth dose
    sheet["BV" + row] = "-" # Tetanus
    sheet["BW" + row] = "-" # Hepatitis A
    sheet["BX" + row] = "-" # Pertussis
    sheet["BY" + row] = "-" # Hepatitis B
    sheet["BZ" + row] = "-" # Diphtheria
    sheet["CA" + row] = "-" # Encephalomeningitis
    sheet["CB" + row] = "-" # Measles/Mumps/Rubella
    sheet["CC" + row] = "-" # Influenza
    sheet["CD" + row] = "-" # Polio
    sheet["CE" + row] = "-" # Chickenpox
    sheet["CF" + row] = "-" # Other
    sheet["CG" + row] = "-" # Shirt Size
    sheet["CH" + row] = "-" # Dietary needs
    sheet["CI" + row] = "-" # Dietary needs - Other
    sheet["CJ" + row] = "-" # The mobility aids that are being brought
    sheet["CK" + row] = "-" # Mobility needs - Other
    sheet["CL" + row] = "-" # Special needs
    sheet["CM" + row] = "-" # Religion
    sheet["CN" + row] = "-" # Religion - Other
    sheet["CO" + row] = "-" # Languages spoken
    sheet["CP" + row] = "-" # Languages spoken � Other
    sheet["CQ" + row] = "-" # "Langauges spoken  - (advanced, intermediate, beginner)"
    sheet["CR" + row] = "-" # Insurance
    sheet["CS" + row] = "-" # Name of insurance company
    sheet["CT" + row] = "-" # Phone number of insurance company
    sheet["CU" + row] = "-" # Insurance certificate
    sheet["CV" + row] = "-" # Prior experience of participating in a WSJ (World Scout Jamboree)
    sheet["CW" + row] = "-" # Prior experience of participating in a WSJ - Other
    sheet["CX" + row] = "-" # Past WSJ role(s)
    sheet["CY" + row] = "-" # Participation in the Pre-Jamboree Activities
    sheet["CZ" + row] = "-" # Boarding the official Jamboree shuttle bus
    sheet["DA" + row] = "-" # Preferred time to arrive at the Jamboree site(Date)
    sheet["DB" + row] = "-" # Preferred time to arrive at the Jamboree site(Time)
    sheet["DC" + row] = "-" # Participation in the Pre-Jamboree Activities
    sheet["DD" + row] = "-" # Boarding the official Jamboree shuttle bus
    sheet["DE" + row] = "-" # Preferred departure time from Jamboree site(Date)
    sheet["DF" + row] = "-" # Preferred departure time from Jamboree site(Time)
    sheet["DG" + row] = "-" # Name of legal guardian
    sheet["DH" + row] = "-" # Relationship of legal guardian with the participant
    sheet["DI" + row] = "-" # Date of parental/guardian consent

    counter += 1
cursor.close()

#save the file
os.makedirs("upload_korea", exist_ok=True)
workbook.save(filename="upload_korea/" + today + "--wsj_insert_de.xlsx")
