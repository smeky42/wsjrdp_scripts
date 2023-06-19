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

  db_people = pd.read_sql_query("""SELECT id, primary_group_id, email, first_name, last_name FROM people 
                                    WHERE status not in ('abgemeldet', 'Abmeldung Vermerkt', 'in Überprüfung durch KT', '')
                                    and role_wish <> '' """, cnx)
  df_people = pd.DataFrame(db_people)

  db_groups = pd.read_sql_query('''SELECT id, name FROM groups''', cnx)
  df_groups = pd.DataFrame(db_groups)
  df_groups = df_groups.rename(columns={'id': 'group_id','name': 'group_name'})

  df = pd.merge(df_people, df_groups, left_on='primary_group_id', right_on='group_id',how='left')

  df_flight = pd.read_excel('2023-05-30--Flüge-Komplett_checked_A-F-triple-checked.xlsx')

  df = pd.merge(df, df_flight, left_on='id', right_on='id',how='left')
  print(df.columns)

  cursor = cnx.cursor()
  rows_affected = 0
  rows_bus_planned_not_needed = 0
  rows_bus_not_planned_but_needed = 0
  # print(df.columns)
  # print(df.iloc[2])
  for row in df.itertuples():
    flight_info = f""""""

    # if isinstance(variable, str) and len(variable) > 1:
    if isinstance(row.outbound_flight_number, str) and len(row.outbound_flight_number) > 1:
      flight_info += f"""
      Hinflug: {row.outbound_flight_number}
      {row.outbound_flight_city_departure}->{row.outbound_flight_city_arrival}
      {row.outbound_flight_date_departure.strftime('%d.%m.%Y %H:%M')}->{row.outbound_flight_date_arrival.strftime('%d.%m.%Y %H:%M')}
      """
    if isinstance(row.outbound_flight_number_stop, str) and len(row.outbound_flight_number_stop) > 1: 
      flight_info += f"""
      Zwischenstop: {row.outbound_flight_number_stop}
      {row.outbound_flight_city_departure_stop}->{row.outbound_flight_city_arrival_stop}
      {row.outbound_flight_date_departure_stop.strftime('%d.%m.%Y %H:%M')}->{row.outbound_flight_date_arrival_stop.strftime('%d.%m.%Y %H:%M')}
      """

    if isinstance(row.inbound_flight_number, str) and len(row.inbound_flight_number) > 1: 
      flight_info += f"""
      Rückflug: {row.inbound_flight_number}
      {row.inbound_flight_city_departure}->{row.inbound_flight_city_arrival}
      {row.inbound_flight_date_departure.strftime('%d.%m.%Y %H:%M')}->{row.inbound_flight_date_arrival.strftime('%d.%m.%Y %H:%M')}
      """
    if isinstance(row.inbound_flight_number_stop, str) and len(row.inbound_flight_number_stop) > 1: 
      flight_info += f"""
      Zwischenstop: {row.inbound_flight_number_stop}
      {row.inbound_flight_city_departure_stop}->{row.inbound_flight_city_arrival_stop}
      {row.inbound_flight_date_departure_stop.strftime('%d.%m.%Y %H:%M')}->{row.inbound_flight_date_arrival_stop.strftime('%d.%m.%Y %H:%M')}
      """

    # print(flight_info)

    update = f"update people set air_travel='{flight_info}' where id='{row.id}'"
    #print(update)
    cursor.execute(update)
    cnx.commit()
    rows_affected += cursor.rowcount
  print (rows_affected, "record(s) affected")
  
if __name__ == "__main__":
    sys.exit(main())
