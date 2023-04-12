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
query =   ("select role_wish first_name, last_name, gender, primary_group_id, zip_code, status from people where id=2;")
cursor.execute(query)
#load excel file
workbook = load_workbook(filename="wsj_insert_en.xlsx")

#open workbook
sheet = workbook.active

counter = 5
for (role_wish, first_name, last_name, gender, primary_group_id, zip_code, status) in cursor:
    row = str(counter)
    sheet["A" + row] = "-" # No. 
    sheet["A" + row] = registration_mapper.type(role_wish) # "Type - (Youth participant, Adult participant)"
    sheet["A" + row] = "-" # Name of NSO
    sheet["A" + row] = registration_mapper.position(role_wish) # Position
    sheet["A" + row] = "-" # Nationality
    sheet["A" + row] = "-" # Hangeul
    sheet["A" + row] = "-" # Roman alphabet
    sheet["A" + row] = "-" # Surname
    sheet["A" + row] = "-" # Middle Name
    sheet["A" + row] = "-" # Given Name
    sheet["A" + row] = "-" # Name on ID card
    sheet["A" + row] = "-" # Gender
    sheet["A" + row] = "-" # Date of birth
    sheet["A" + row] = "-" # Participant's email
    sheet["A" + row] = "-" # Your affiliation(Scouting)
    sheet["A" + row] = "-" # Job/position
    sheet["A" + row] = "-" # Current position within the NSO
    sheet["A" + row] = "-" # Home address
    sheet["A" + row] = "-" # City
    sheet["A" + row] = "-" # State/Province
    sheet["A" + row] = "-" # Nationality(City)
    sheet["A" + row] = "-" # Zip code
    sheet["A" + row] = "-" # Home phone number
    sheet["A" + row] = "-" # "Mobile phone number - (Country code)"
    sheet["A" + row] = "-" # "Mobile phone number - (phone number)"
    sheet["A" + row] = "-" # "SNS ID - (social media account)"
    sheet["A" + row] = "-" # SNS URL
    sheet["A" + row] = "-" # Name of legal guardian
    sheet["A" + row] = "-" # Phone number of legal guardian
    sheet["A" + row] = "-" # Email address of legal guardian 
    sheet["A" + row] = "-" # Primart emergency contact name
    sheet["A" + row] = "-" # "Relationship with primary  - emergency contact"
    sheet["A" + row] = "-" # "Primary emergency  - contact phone number"
    sheet["A" + row] = "-" # "secondart emergency  - contact name"
    sheet["A" + row] = "-" # Relationship with secondary emergency contact
    sheet["A" + row] = "-" # Secondary emergency contact phone number
    sheet["A" + row] = "-" # Passport number
    sheet["A" + row] = "-" # Date of issue
    sheet["A" + row] = "-" # Valid until
    sheet["A" + row] = "-" # Passport issuing country
    sheet["A" + row] = "-" # Means of transportation
    sheet["A" + row] = "-" # Ariline
    sheet["A" + row] = "-" # Date of departure
    sheet["A" + row] = "-" # Arrival airport
    sheet["A" + row] = "-" # Date of arrival
    sheet["A" + row] = "-" # Time of arrival
    sheet["A" + row] = "-" # Flight number
    sheet["A" + row] = "-" # Origin point / Last city of boarding
    sheet["A" + row] = "-" # Date of departure
    sheet["A" + row] = "-" # departure time
    sheet["A" + row] = "-" # Blood type
    sheet["A" + row] = "-" # Blood type - Other 
    sheet["A" + row] = "-" # Underlying health conditions
    sheet["A" + row] = "-" # Underlying health conditions - Other
    sheet["A" + row] = "-" # History of surgery or hospitalization
    sheet["A" + row] = "-" # Name of medication 
    sheet["A" + row] = "-" # Dosage
    sheet["A" + row] = "-" # Frequency
    sheet["A" + row] = "-" # Reason for medication intake 
    sheet["A" + row] = "-" # Allergies
    sheet["A" + row] = "-" # Allergies � Other 
    sheet["A" + row] = "-" # Allergies � specific details
    sheet["A" + row] = "-" # Food allergies
    sheet["A" + row] = "-" # Food allergies - Other
    sheet["A" + row] = "-" # "Types of COVID-19 vaccines �  - first dose"
    sheet["A" + row] = "-" # "Types of COVID-19 vaccines �  - Second dose"
    sheet["A" + row] = "-" # "Types of COVID-19 vaccines �  - Third dose"
    sheet["A" + row] = "-" # "Types of COVID-19 vaccines �  - Fourth dose"
    sheet["A" + row] = "-" # Dates vaccinated � First dose
    sheet["A" + row] = "-" # Dates vaccinated � Seconf dose
    sheet["A" + row] = "-" # Dates vaccinated � Third dose
    sheet["A" + row] = "-" # Dates vaccinated � Fourth dose
    sheet["A" + row] = "-" # Tetanus
    sheet["A" + row] = "-" # Hepatitis A
    sheet["A" + row] = "-" # Pertussis
    sheet["A" + row] = "-" # Hepatitis B
    sheet["A" + row] = "-" # Diphtheria
    sheet["A" + row] = "-" # Encephalomeningitis 
    sheet["A" + row] = "-" # Measles/Mumps/Rubella 
    sheet["A" + row] = "-" # Influenza
    sheet["A" + row] = "-" # Polio
    sheet["A" + row] = "-" # Chickenpox
    sheet["A" + row] = "-" # Other
    sheet["A" + row] = "-" # Shirt Size
    sheet["A" + row] = "-" # Dietary needs
    sheet["A" + row] = "-" # Dietary needs - Other
    sheet["A" + row] = "-" # The mobility aids that are being brought
    sheet["A" + row] = "-" # Mobility needs - Other 
    sheet["A" + row] = "-" # Special needs  
    sheet["A" + row] = "-" # Religion
    sheet["A" + row] = "-" # Religion - Other
    sheet["A" + row] = "-" # Languages spoken 
    sheet["A" + row] = "-" # Languages spoken � Other 
    sheet["A" + row] = "-" # "Langauges spoken  - (advanced, intermediate, beginner)"
    sheet["A" + row] = "-" # Insurance
    sheet["A" + row] = "-" # Name of insurance company
    sheet["A" + row] = "-" # Phone number of insurance company 
    sheet["A" + row] = "-" # Insurance certificate 
    sheet["A" + row] = "-" # Prior experience of participating in a WSJ (World Scout Jamboree) 
    sheet["A" + row] = "-" # Prior experience of participating in a WSJ - Other
    sheet["A" + row] = "-" # Past WSJ role(s) 
    sheet["A" + row] = "-" # Participation in the Pre-Jamboree Activities
    sheet["A" + row] = "-" # Boarding the official Jamboree shuttle bus
    sheet["A" + row] = "-" # Preferred time to arrive at the Jamboree site(Date)
    sheet["A" + row] = "-" # Preferred time to arrive at the Jamboree site(Time)
    sheet["A" + row] = "-" # Participation in the Pre-Jamboree Activities
    sheet["A" + row] = "-" # Boarding the official Jamboree shuttle bus
    sheet["A" + row] = "-" # Preferred departure time from Jamboree site(Date)
    sheet["A" + row] = "-" # Preferred departure time from Jamboree site(Time)
    sheet["A" + row] = "-" # Name of legal guardian 
    sheet["A" + row] = "-" # Relationship of legal guardian with the participant
    sheet["A" + row] = "-" # Date of parental/guardian consent

    counter += 1
cursor.close()

#save the file
workbook.save(filename="upload_korea/" + today + "--wsj_insert_de.xlsx")

