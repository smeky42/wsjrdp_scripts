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

  db_people = pd.read_sql_query("""SELECT id, primary_group_id, role_wish, 
                                    medicine_preexisting_conditions,
                                    medicine_abnormalities, 
                                    medicine_allergies, 
                                    medicine_eating_disorders, 
                                    medicine_mobility_needs, 
                                    medicine_infectious_diseases, 
                                    medicine_medical_treatment_contact, 
                                    medicine_continous_medication, 
                                    medicine_needs_medication, 
                                    medicine_medications_self_treatment, 
                                    medicine_other, 
                                    medicine_important, 
                                    medicine_support,
                                    CONCAT('https://anmeldung.worldscoutjamboree.de/groups/' , primary_group_id , '/people/' , id) as link
                                    FROM people 
                                    WHERE status not in ('abgemeldet', 'Abmeldung Vermerkt', 'in Überprüfung durch KT', '')""", cnx)
  df_people = pd.DataFrame(db_people)

  df = df_people
  # db_groups = pd.read_sql_query('''SELECT id, name FROM groups''', cnx)
  # df_groups = pd.DataFrame(db_groups)
  # df_groups = df_groups.rename(columns={'id': 'group_id','name': 'group_name'})

  # df = pd.merge(df_people, df_groups, left_on='primary_group_id', right_on='group_id',how='left')

  
  # df.drop(["primary_group_id","role_wish","person_id",], axis=1, inplace=True)
  # agg_functions = {'id': 'count', 'medicine_eating_disorders': 'sum', 'travel_date': 'first', 'travel_day_time': 'first'}
  # df = df.groupby(['travel_date','travel_day_time']).aggregate(agg_functions)

  print(df)
  # print(df.iloc[2])
    


  df.to_excel(f"{str(date.today())}--Medikamente.xlsx", sheet_name="Medizin", index=False)

if __name__ == "__main__":
    sys.exit(main())
