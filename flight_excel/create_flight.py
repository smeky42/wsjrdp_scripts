#!/usr/bin/env python
import sys
from datetime import date


import yaml
from mysql.connector import connection
import pandas as pd

def update_short_name(row):
   row["short_name"] = row['short_group_name'].split(' ')[0]
   return row

def update_outbound(row, df):
  dfg = df[(df['Unit'] == row["short_name"])]
  if len(dfg) > 0:
    irow = dfg[(df['# Flug'] == 1)].iloc[0] 
    row["outbound_flight_number"] = irow["Flugnummer"]
    row["outbound_flight_city_departure"] = irow["Distanz (Ort_Ort)"].split('_')[0]
    row["outbound_flight_date_departure"] = irow["Abflugdatum "].replace(hour=irow["Abflugzeit"].hour, minute=irow["Abflugzeit"].minute)
    row["outbound_flight_city_arrival"] = irow["Distanz (Ort_Ort)"].split('_')[1]
    row["outbound_flight_date_arrival"] = irow["Ankunftsdatum"].replace(hour=irow["Ankunftszeit"].hour, minute=irow["Ankunftszeit"].minute)
  
  if len(dfg) == 4:
    irow = dfg[(df['# Flug'] == 2)].iloc[0] 
    row['outbound_flight_number_stop'] = irow["Flugnummer"] 
    row["outbound_flight_city_departure_stop"] = irow["Distanz (Ort_Ort)"].split('_')[0]
    row['outbound_flight_date_departure_stop'] = irow["Abflugdatum "].replace(hour=irow["Abflugzeit"].hour, minute=irow["Abflugzeit"].minute) 
    row['outbound_flight_city_arrival_stop'] = irow["Distanz (Ort_Ort)"].split('_')[1] 
    row['outbound_flight_date_arrival_stop'] = irow["Ankunftsdatum"].replace(hour=irow["Ankunftszeit"].hour, minute=irow["Ankunftszeit"].minute) 
 
  return row

def update_inbound(row, df):
  dfg = df[(df['Unit'] == row["short_name"])]
  if len(dfg) == 2:
    irow = dfg[(df['# Flug'] == 2)].iloc[0] 
    row["inbound_flight_number"] = irow["Flugnummer"]
    row["inbound_flight_city_departure"] = irow["Distanz (Ort_Ort)"].split('_')[0]
    row["inbound_flight_date_departure"] = irow["Abflugdatum "].replace(hour=irow["Abflugzeit"].hour, minute=irow["Abflugzeit"].minute)
    row["inbound_flight_city_arrival"] = irow["Distanz (Ort_Ort)"].split('_')[1]
    row["inbound_flight_date_arrival"] = irow["Ankunftsdatum"].replace(hour=irow["Ankunftszeit"].hour, minute=irow["Ankunftszeit"].minute)
  
  if len(dfg) == 4:
    irow = dfg[(df['# Flug'] == 3)].iloc[0] 
    row["inbound_flight_number"] = irow["Flugnummer"]
    row["inbound_flight_city_departure"] = irow["Distanz (Ort_Ort)"].split('_')[0]
    row["inbound_flight_date_departure"] = irow["Abflugdatum "].replace(hour=irow["Abflugzeit"].hour, minute=irow["Abflugzeit"].minute)
    row["inbound_flight_city_arrival"] = irow["Distanz (Ort_Ort)"].split('_')[1]
    row["inbound_flight_date_arrival"] = irow["Ankunftsdatum"].replace(hour=irow["Ankunftszeit"].hour, minute=irow["Ankunftszeit"].minute)

    irow = dfg[(df['# Flug'] == 4)].iloc[0] 
    row['inbound_flight_number_stop'] = irow["Flugnummer"] 
    row["inbound_flight_city_departure_stop"] = irow["Distanz (Ort_Ort)"].split('_')[0]
    row['inbound_flight_date_departure_stop'] = irow["Abflugdatum "].replace(hour=irow["Abflugzeit"].hour, minute=irow["Abflugzeit"].minute) 
    row['inbound_flight_city_arrival_stop'] = irow["Distanz (Ort_Ort)"].split('_')[1] 
    row['inbound_flight_date_arrival_stop'] = irow["Ankunftsdatum"].replace(hour=irow["Ankunftszeit"].hour, minute=irow["Ankunftszeit"].minute) 
 
  return row





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

  db = pd.read_sql_query('''SELECT id, primary_group_id, first_name, last_name, role_wish 
                                    FROM people 
                                    WHERE role_wish <> "" 
                                    AND status NOT IN ("abgemeldet", "Abmeldung Vermerkt", "in Überprüfung durch KT", "")''', cnx)
  dfp = pd.DataFrame(db)
  # print(dfh.columns)

  db_groups = pd.read_sql_query('''SELECT id, short_name FROM groups''', cnx)
  df_groups = pd.DataFrame(db_groups)
  df_groups = df_groups.rename(columns={'id': 'group_id','short_name': 'short_group_name'})

  dfp = pd.merge(dfp, df_groups, left_on='primary_group_id', right_on='group_id',how='left')

  dfi = pd.read_excel("20230501_Fluege_Update.xlsx")
  # print(dfk.columns)

  print(dfi[(dfi['Unit']=='C4') & (dfi['# Flug'] == 1)].at[0, "Flugnummer"])

  dfp["short_name"] = None 
  dfp = dfp.apply(update_short_name, axis=1)

  dfp['outbound_flight_number'] = None
  dfp['outbound_flight_city_departure'] = None 
  dfp['outbound_flight_date_departure'] = None 
  dfp['outbound_flight_city_arrival'] = None 
  dfp['outbound_flight_date_arrival'] = None 
  
  dfp['outbound_flight_number_stop'] = None 
  dfp['outbound_flight_city_departure_stop'] = None 
  dfp['outbound_flight_date_departure_stop'] = None 
  dfp['outbound_flight_city_arrival_stop'] = None 
  dfp['outbound_flight_date_arrival_stop'] = None 

  dfp = dfp.apply(update_outbound, axis=1, df=dfi)
  
  dfp['inbound_flight_number'] = None
  dfp['inbound_flight_city_departure'] = None 
  dfp['inbound_flight_date_departure'] = None 
  dfp['inbound_flight_city_arrival'] = None 
  dfp['inbound_flight_date_arrival'] = None 
  
  dfp['inbound_flight_number_stop'] = None 
  dfp['inbound_flight_city_departure_stop'] = None 
  dfp['inbound_flight_date_departure_stop'] = None 
  dfp['inbound_flight_city_arrival_stop'] = None 
  dfp['inbound_flight_date_arrival_stop'] = None 

  dfp = dfp.apply(update_inbound, axis=1, df=dfi)

  
  #print(dfp[["outbound_flight_number","outbound_flight_city_departure","outbound_flight_date_departure"]])
  columnsTitles = [ "id", 
 "role_wish", 
 "first_name", 
 "primary_group_id", 
 "short_group_name", 
 "short_name"
 "last_name", 
 "outbound_flight_number", 
 "outbound_flight_date_departure", 
 "outbound_flight_city_departure", 
 "outbound_flight_date_arrival", 
 "outbound_flight_city_arrival", 
 "outbound_flight_number_stop", 
 "outbound_flight_date_departure_stop", 
 "outbound_flight_city_departure_stop", 
 "outbound_flight_date_arrival_stop", 
 "outbound_flight_city_arrival_stop", 
 "inbound_flight_number", 
 "inbound_flight_date_departure", 
 "inbound_flight_city_departure", 
 "inbound_flight_date_arrival", 
 "inbound_flight_city_arrival", 
 "inbound_flight_number_stop", 
 "inbound_flight_date_departure_stop", 
 "inbound_flight_city_departure_stop", 
 "inbound_flight_date_arrival_stop", 
 "inbound_flight_city_arrival_stop"]
  
  dfp.to_excel(f"{str(date.today())}--Flüge-Komplett.xlsx", sheet_name="Komplett", index=False)

if __name__ == "__main__":
    sys.exit(main())
