#!/usr/bin/env python
import os
import sys
import typing
import warnings
from datetime import date

import yaml
from mysql.connector import connection
from openpyxl import load_workbook

import json
from registration_person import RegistrationPerson



def main():
    with open("../config.yml", "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read successful")

    # today = str(date.today())
    cnx = connection.MySQLConnection(
        user=config["username"],
        password=config["password"],
        host="anmeldung.worldscoutjamboree.de",
        port=config["port"],
        database=config["database"],
    )

    cursor = cnx.cursor()

    where_clause = ("id > 1 "
                    "and role_wish = 'IST' "
                    "and status not in ('abgemeldet', 'Abmeldung Vermerkt', 'in Überprüfung durch KT', '')")

    cursor.execute(f"select id, primary_group_id from people where {where_clause}; ")
    rows = cursor.fetchall()
    print("Read database")

    counter = 1
    for row in rows:
      if row[1] == 60: #Foodhouse 60 / GDV 59
        update = f"insert into ist_jobs (id,subject_id, author_id, first_choice, first_specialization, second_choice, second_specialization, third_choice, third_specialization, created_at) values ({(593 + counter)},'{row[0]}',2, 'IN-7-2 Food House', 'German Black Tent Foodhouse Prealloc', 'IN-7-2 Food House', 'German Black Tent Foodhouse Prealloc', 'IN-7-2 Food House', 'German Black Tent Foodhouse Prealloc', '2023-04-22 14:23:42')"
        # update = f"insert into ist_jobs (id,subject_id, author_id, first_choice, first_specialization, second_choice, second_specialization, third_choice, third_specialization, created_at) values ({(585 + counter)},'{row[0]}',2, 'OT-1-1 Preallocated ISTs', 'GDV', 'OT-1-1 Preallocated ISTs', 'GDV', 'OT-1-1 Preallocated ISTs', 'GDV', '2023-04-22 13:42:23')"
        counter += 1
        print(update)
        cursor.execute(update)
        cnx.commit()
        print (cursor.rowcount, "record(s) affected")


    cursor.close()



if __name__ == "__main__":
    sys.exit(main())




 
