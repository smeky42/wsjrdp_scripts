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

  db_people = pd.read_sql_query('''SELECT id, primary_group_id, first_name, last_name, shirt_size, role_wish, head, kola_participation 
                                    FROM people 
                                    WHERE role_wish <> "" 
                                    AND status NOT IN ("abgemeldet", "Abmeldung Vermerkt", "in Überprüfung durch KT", "")''', cnx)
  df_people = pd.DataFrame(db_people)

  db_groups = pd.read_sql_query('''SELECT id, name FROM groups''', cnx)
  df_groups = pd.DataFrame(db_groups)
  df_groups = df_groups.rename(columns={'id': 'group_id','name': 'group_name'})

  df = pd.merge(df_people, df_groups, left_on='primary_group_id', right_on='group_id',how='left')

  df_trigema = pd.read_excel("Trigema-Größen.xlsx")
  # df_event.drop(["primary_group_id","role_wish","medicine_eating_disorders","first_name", "last_name",], axis=1, inplace=True)
  df = pd.merge(df, df_trigema, left_on='id', right_on='ID',how='left')

  print(df)
  # print(df.iloc[2])
    


  df.to_excel(f"{str(date.today())}--Trigema-Names.xlsx", sheet_name="Komplett", index=False)

if __name__ == "__main__":
    sys.exit(main())
