#!/usr/bin/env python
import sys
from datetime import date

import yaml
from mysql.connector import connection
import pandas as pd

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

  db_h = pd.read_sql_query('''SELECT id, primary_group_id, first_name, last_name, role_wish, korea_id 
                                    FROM people 
                                    WHERE role_wish <> "" 
                                    AND status NOT IN ("abgemeldet", "Abmeldung Vermerkt", "in Überprüfung durch KT", "")''', cnx)
  dfh = pd.DataFrame(db_h)
  # print(dfh.columns)

  dfk = pd.read_excel("korea_data/Apply_Basic(2023-06-01)-format.xls", header=1)
  dfk = dfk[dfk['Status'] != 'Cancelled']
  # print(dfk.columns)

  print("== anmeldung vs. korea data ==")
  print(f"ALL:\t{len(dfh)} \t {len(dfk)}")
  print(f"CMT:\t{len(dfh[dfh['role_wish']=='Kontingentsteam'])} \t {len(dfk[dfk['Position']=='CMT'])}")
  print(f"IST:\t{len(dfh[dfh['role_wish']=='IST'])} \t {len(dfk[dfk['Position']=='IST'])}")
  print(f"UL:\t{len(dfh[dfh['role_wish']=='Unit Leitung'])} \t {len(dfk[dfk['Position']=='Unit Leader'])}")
  print(f"TN:\t{len(dfh[dfh['role_wish']=='Teilnehmende*r'])} \t {len(dfk[dfk['Position']=='Youth Participant'])}")
  print(f"JPT:\t{len(dfh[dfh['role_wish']=='JPT'])} \t {len(dfk[dfk['Position']=='JPT'])}")

  print("== rows which are not in both dataframes ==")
  merged = dfh.merge(dfk, left_on='korea_id', right_on='ID number', how='outer', indicator=True)
  diff = merged[merged['_merge'] != 'both'].drop('_merge', axis=1)
  print(diff[['id','role_wish','first_name','last_name','ID number','Position','Given Name','Surname', 'Status']])

  print(f"{len(diff)} rows differ")
if __name__ == "__main__":
    sys.exit(main())
