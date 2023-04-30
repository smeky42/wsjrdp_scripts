#!/usr/bin/env python
import sys
from datetime import date

import yaml
from mysql.connector import connection
#from openpyxl import load_workbook
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

  db_people = pd.read_sql_query('''SELECT id, first_name, last_name FROM people LIMIT 10''', cnx)
  df_people = pd.DataFrame(db_people)
  print(df_people.columns)
  print(df_people.head(10))

  df_bus = pd.read_excel('2023-04-29--Busliste.xlsx')
  df_bus = df_bus.rename(columns={'TN ID': 'tn_id', 'Adresse': 'adresse', 'PLZ': 'plz', 'Ort': 'ort', 'Bus Route': 'bus_route', 'Busnummer': 'busnummer',
       'Hinfahrt Abfahrtsort': 'hinfahrt_abfahrtsort', 'Definitiver Abfahrtsort': 'definitiver_abfahrtsort', 'Abfahrtszeit': 'abfahrtszeit',
       'Ankunftszeit Immenhausen': 'ankunftszeit_immenhausen', 'Abfahrt Immenhausen am Sonntag ': 'abfahrt_immenhausen_am_sonntag',
       'Ankunftsort Rückfahrt': 'ankunftsort_rückfahrt', 'Ankunftszeit Rückfahrt': 'ankunftszeit_rückfahrt',
       'Zusätzliches Material Gepäck': 'zusätzliches_material_gepäck', 'Hinweise': 'hinweise'})
  print(df_bus.columns)

  df = pd.merge(df_people, df_bus, left_on='id', right_on='tn_id',how='left')
  print(df.columns)
  # print(df_bus['TN ID'].to_string(index=False))

  for row in df.itertuples():
     print(row.hinfahrt_abfahrtsort)  

if __name__ == "__main__":
    sys.exit(main())
