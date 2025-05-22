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

  db_people = pd.read_sql_query("""SELECT id, primary_group_id, role_wish, medicine_eating_disorders FROM people 
                                    WHERE status not in ('abgemeldet', 'Abmeldung Vermerkt', 'in Überprüfung durch KT', '')
                                    and (role_wish="IST" or role_wish="Kontingentsteam" or role_wish="" ) 
                                    and kola_participation=1""", cnx)
  df_people = pd.DataFrame(db_people)

  db_groups = pd.read_sql_query('''SELECT id, name FROM groups''', cnx)
  df_groups = pd.DataFrame(db_groups)
  df_groups = df_groups.rename(columns={'id': 'group_id','name': 'group_name'})

  df = pd.merge(df_people, df_groups, left_on='primary_group_id', right_on='group_id',how='left')

  # person_id, primary_group_id, role_wish, first_name, last_name, medicine_eating_disorders, bus_travel, travel_date, travel_day_time
  df_event = pd.read_csv("../bus/2023-05-01--09-15-14---KoLa-Anmeldung-IST-CMT.csv")
  df_event.drop(["primary_group_id","role_wish","medicine_eating_disorders","first_name", "last_name",], axis=1, inplace=True)
  df = pd.merge(df, df_event, left_on='id', right_on='person_id',how='left')

  df = df.sort_values(by=['travel_date','travel_day_time'], ascending=[True, False])

  df.drop(["primary_group_id","role_wish","person_id",], axis=1, inplace=True)
  # agg_functions = {'id': 'count', 'medicine_eating_disorders': 'sum', 'travel_date': 'first', 'travel_day_time': 'first'}
  # df = df.groupby(['travel_date','travel_day_time']).aggregate(agg_functions)

  print(df)
  # print(df.iloc[2])
    


  df.to_excel(f"{str(date.today())}--IST-KT-Anreise-Essen-Immenhausen.xlsx", sheet_name="Busreise", index=False)

if __name__ == "__main__":
    sys.exit(main())
