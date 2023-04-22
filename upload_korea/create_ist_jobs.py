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

import registration_mapper
from registration_person import RegistrationPerson

def get_department(choice):
    if choice.startswith("PL"):
        return "1"
    if choice.startswith("PG"):
        return "2"
    if choice.startswith("SP"):
        return "3"
    if choice.startswith("SF"):
        return "4"
    if choice.startswith("IN"):
        return "5"
    if choice.startswith("OT"):
        return "6"
    
    return ""

def to_sheet_row_dict(p: RegistrationPerson, j: dict, no: int) -> dict[str, dict, typing.Any]:
    
    job_entry ={"id": "","first_choice":"","first_specialization":"","second_choice":"","second_specialization":"","third_choice":"","third_specialization":""}

    for ist_job in j:
        if ist_job.get("subject_id") == p.id:
          job_entry  = ist_job 
          break 

    d: dict[str, typing.Any] = {}
    # fmt: off
    d["A"] = str(no)  # No.
    d["B"] = p.korea_id #ID Number
    d["C"] = p.name_on_id_card  # Name on ID card X
    d["D"] = "57"  # Name of NSO X
    d["E"] = get_department(job_entry.get("first_choice")) # First job preference (Department)	
    d["F"] = job_entry.get("first_choice").split("\t")[0].split(" ")[0]  # First job preference (Team)
    d["G"] = job_entry.get("first_specialization")  # specialization(1st) (Major, Experience and Certificate)
    d["H"] = get_department(job_entry.get("second_choice")) # Second job preference (Department)	
    d["I"] = job_entry.get("second_choice").split("\t")[0].split(" ")[0]  # Second job preference (Team)
    d["J"] = job_entry.get("second_specialization")  # specialization(2nd) (Major, Experience and Certificate)
    d["K"] = get_department(job_entry.get("third_choice")) # Third job preference (Department)	
    d["L"] = job_entry.get("third_choice").split("\t")[0].split(" ")[0]  # Third job preference (Team)
    d["M"] = job_entry.get("third_specialization")  # specialization(3rd) (Major, Experience and Certificate)
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

    cursor = cnx.cursor(dictionary=True)
    cursor.execute("select * from ist_jobs order by id desc")
    ist_jobs = cursor.fetchall()
    
    

    


    # where_clause = "role_wish = 'Teilnehmende*r' limit 200"
    # where_clause = ""and id not in (2, 2432, 2428, 2413, 2386, 2375, 2360, 1912, 1810, 626, 625, 312)""
    # where_clause = "id=2"
    # where_clause = "id > 2 and (status = 'bestätigt durch KT' or status = 'bestätigt durch Leitung' or status = 'vollständig')"
    where_clause = ("id > 1 "
                    "and role_wish = 'IST' "
                    "and status not in ('abgemeldet', 'Abmeldung Vermerkt', 'in Überprüfung durch KT', '')")


    cursor.execute(RegistrationPerson.get_db_query(where_clause))

    print("Read database")

    # load excel file
    workbook = load_workbook(filename="ist_job_assignment.xlsx")

    # open workbook
    sheet: typing.Any = workbook.active

    counter = 0
    for counter, row_dict in enumerate(cursor, start=1):
        p = RegistrationPerson(**row_dict)
        # Catch all warnings generated while collecting the data for
        # the next row in the sheet.
        with warnings.catch_warnings(record=True) as warnings_list:
            sheet_row_dict = to_sheet_row_dict(p, ist_jobs, no=counter)

        # If we got some warnings, print them
        if warnings_list:
            print(f"Error(s): id={p.id} {p.first_name} {p.last_name}")
            for warning_item in warnings_list:
                print(f"  - {warning_item.message}")

        # write row data into sheet
        row = str(counter + 3)
        for col, val in sheet_row_dict.items():
            if val is not None:
                sheet[col + row] = val

    cursor.close()

    # save the file
    # os.makedirs("upload_korea", exist_ok=True)
    workbook.save(filename=today + "--ist_job_assignment_de.xlsx")
    print(f"Wrote {counter} rows")


if __name__ == "__main__":
    sys.exit(main())
