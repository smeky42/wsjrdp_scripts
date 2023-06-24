#!/usr/bin/env python
import sys
from openpyxl import load_workbook
from mysql.connector import (connection)
from datetime import date
import yaml

def food_allergies(medicine_allergies: str | None, medicine_eating_disorders: str | None):    
    food_allergies_str = medicine_eating_disorders.lower() + medicine_allergies.lower()
    # vegan, vegetarisch, gluten frei, lactose intolerant, kosher, halal 
    food_allergies_map = ""

    if "vegan" in food_allergies_str:
        food_allergies_map += "vegan "
    if "vege" in food_allergies_str:
        food_allergies_map += "vegetarisch "
    if "gluten" in food_allergies_str:
        food_allergies_map += "glutenfrei "
    if "kosher" in food_allergies_str:
        food_allergies_map += "kosher "
    if "halal" in food_allergies_str:
        food_allergies_map += "halal "
    if "lactose" in food_allergies_str: 
        food_allergies_map += "lactoseintolerant "

    return food_allergies_map


def main():
  with open("../config.yml", "r") as yamlfile:
      config = yaml.load(yamlfile, Loader=yaml.FullLoader)
      print("Read successful")

  today = str(date.today())
  cnx = connection.MySQLConnection(user=config['username'], password=config['password'],
                                  host='anmeldung.worldscoutjamboree.de',
                                  port=config['port'],
                                  database=config['database'])
  cursor = cnx.cursor()
  query =   ("select g.id, g.name, g.short_name from groups g")
  cursor.execute(query)
  groups = cursor.fetchall()
  cursor.close 

  for (id, name, short_name) in groups:
      print(str(id) + name + short_name)
      if id in [1,2,3,4,5,6,53,56,57,58]:
          continue
      
      cursor = cnx.cursor()
      query =   ("select first_name, last_name, gender, primary_group_id, zip_code, status, rail_and_fly,rail_and_fly_reason, medicine_eating_disorders, medicine_allergies from people where primary_group_id=" + str(id) + ";")
      cursor.execute(query)
      #load excel file
      workbook = load_workbook(filename="CT-Namenserfassung-Gruppen.xlsm")
      
      #open workbook
      sheet = workbook.active

      counter = 5
      for (first_name, last_name, gender, primary_group_id, zip_code, status, rail_and_fly,rail_and_fly_reason, medicine_eating_disorders, medicine_allergies ) in cursor:
          sheet["G" + str(counter)] = gender.replace("m","Mr").replace("w","Mrs")
          sheet["H" + str(counter)] = first_name
          sheet["I" + str(counter)] = last_name
          sheet["M" + str(counter)] = rail_and_fly
          sheet["N" + str(counter)] = rail_and_fly_reason
          sheet["O" + str(counter)] = food_allergies(medicine_eating_disorders, medicine_allergies)
          
          counter += 1
      cursor.close()
      
      #save the file
      workbook.save(filename=today + "--" + str(id) + "-Flight-" + short_name.replace(" ", "-") + ".xlsx")


  cnx.close()


if __name__ == "__main__":
  sys.exit(main())