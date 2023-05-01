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

  db_people = pd.read_sql_query("""SELECT id, primary_group_id, first_name, last_name, kola_participation, kola_bus, kola_reason FROM people 
                                    WHERE status not in ('abgemeldet', 'Abmeldung Vermerkt', 'in Überprüfung durch KT', '')
                                    and role_wish <> '' """, cnx)
  df_people = pd.DataFrame(db_people)

  db_groups = pd.read_sql_query('''SELECT id, name FROM groups''', cnx)
  df_groups = pd.DataFrame(db_groups)
  df_groups = df_groups.rename(columns={'id': 'group_id','name': 'group_name'})

  df = pd.merge(df_people, df_groups, left_on='primary_group_id', right_on='group_id',how='left')

  df_bus = pd.read_excel('2023-04-29--Busliste.xlsx')
  df_bus = df_bus.rename(columns={'TN ID': 'tn_id', 'Adresse': 'adresse', 'PLZ': 'plz', 'Ort': 'ort', 'Bus Route': 'bus_route', 'Busnummer': 'busnummer',
       'Hinfahrt Abfahrtsort': 'hinfahrt_abfahrtsort', 'Definitiver Abfahrtsort': 'definitiver_abfahrtsort', 'Abfahrtszeit': 'abfahrtszeit',
       'Ankunftszeit Immenhausen': 'ankunftszeit_immenhausen', 'Abfahrt Immenhausen am Sonntag ': 'abfahrt_immenhausen_am_sonntag',
       'Ankunftsort Rückfahrt': 'ankunftsort_rueckfahrt', 'Ankunftszeit Rückfahrt': 'ankunftszeit_rueckfahrt',
       'Zusätzliches Material Gepäck': 'zusätzliches_material_gepaeck', 'Hinweise': 'hinweise'})

  df = pd.merge(df, df_bus, left_on='id', right_on='tn_id',how='left')
  #print(df.columns)

  cursor = cnx.cursor()
  rows_affected = 0
  for row in df.itertuples():
    #print(row)
    if pd.isna(row.abfahrtszeit):
       bus_travel = "Für dich ist keine Busanreise zum KoLa geplant."
    else:
      bus_travel = f"""Hinfahrt: {row.abfahrtszeit} {row.definitiver_abfahrtsort} -> {row.ankunftszeit_immenhausen} Immenhausen \n 
      Rückfahrt: {row.abfahrt_immenhausen_am_sonntag} Immenhausen -> {row.ankunftszeit_rueckfahrt} {row.ankunftsort_rueckfahrt}"""
    
    update = f"update people set bus_travel='{bus_travel}' where id='{row.id}'"
    #print(update)
    cursor.execute(update)
    cnx.commit()
    rows_affected += cursor.rowcount
  print (rows_affected, "record(s) affected")

  df.to_excel(f"{str(date.today())}--Bus-Immenhausen.xlsx", sheet_name="Busreise", index=False)

if __name__ == "__main__":
    sys.exit(main())
