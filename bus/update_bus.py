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

  db_people = pd.read_sql_query("""SELECT id, primary_group_id, first_name, last_name, role_wish, birthday, kola_participation, kola_bus, kola_reason FROM people 
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
  df['bus_planned_not_needed'] = 0
  df['bus_not_planned_but_needed'] = 0
  df['bus_info_in_db'] = ""
  #print(df.columns)

  # person_id, primary_group_id, role_wish, first_name, last_name, medicine_eating_disorders, bus_travel, travel_date, travel_day_time
  df_event = pd.read_csv("2023-05-01--09-15-14---KoLa-Anmeldung-IST-CMT.csv")
  df_event.drop(["primary_group_id","role_wish","medicine_eating_disorders","first_name", "last_name",], axis=1, inplace=True)
  df = pd.merge(df, df_event, left_on='id', right_on='person_id',how='left')


  cursor = cnx.cursor()
  rows_affected = 0
  rows_bus_planned_not_needed = 0
  rows_bus_not_planned_but_needed = 0
  # print(df.columns)
  # print(df.iloc[2])
  for row in df.itertuples():
    bus_info_in_db = "Für dich liegen keine Informationen vor, melde dich bitte bei logistik@worldscoutjamboree.de"
    bus_planned = not pd.isna(row.busnummer)
    bus_needed = row.kola_participation == 1 and row.kola_bus == 1 and not row.bus_travel == "nein"
    # bus_false_date = row.bus_travel == "ja" and not str(row.travel_date).__contains__("18.5.")

    bus_planned_not_needed = bus_planned and not bus_needed
    bus_not_planned_but_needed = not bus_planned and bus_needed

    if bus_planned:
      bus_info_in_db = f"""Hinfahrt: 18.5. {row.abfahrtszeit} {row.definitiver_abfahrtsort} -> {row.ankunftszeit_immenhausen} Immenhausen \n 
      Rückfahrt: 21.5. {row.abfahrt_immenhausen_am_sonntag} Immenhausen -> {row.ankunftszeit_rueckfahrt} {row.ankunftsort_rueckfahrt}"""

    if not bus_needed:
       bus_info_in_db = "Für dich ist keine Busanreise zum KoLa geplant."

    if bus_planned_not_needed:
      bus_info_in_db = "Für dich ist keine Busanreise zum KoLa geplant."
      df.at[row.Index, "bus_planned_not_needed"] = 1
      rows_bus_planned_not_needed += 1

    if bus_not_planned_but_needed:
       bus_info_in_db = "Für dich ist, bis jetzt, keine Busanreise zum KoLa geplant."
       df.at[row.Index, "rows_bus_not_planned_but_needed"] = 1
       rows_bus_not_planned_but_needed += 1

    # if bus_false_date:
    #    bus_info_in_db = "Du hast angegeben früher anzureisen Busse fahren aber nur am 18.5., melde dich bitte bei logistik@worldscoutjamboree.de"
       
    df.at[row.Index, "bus_info_in_db"] = bus_info_in_db

    update = f"update people set bus_travel='{bus_info_in_db}' where id='{row.id}'"
    #print(update)
    cursor.execute(update)
    cnx.commit()
    rows_affected += cursor.rowcount
  print (rows_affected, "record(s) affected")
  print (rows_bus_planned_not_needed, "record(s) have a bus_planned_not_needed")
  print (rows_bus_not_planned_but_needed, "record(s) have a rows_bus_not_planned_but_needed")

  df.to_excel(f"{str(date.today())}--Bus-Immenhausen.xlsx", sheet_name="Busreise", index=False)

if __name__ == "__main__":
    sys.exit(main())
