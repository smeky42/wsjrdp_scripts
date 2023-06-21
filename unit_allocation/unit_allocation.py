#!/usr/bin/env python
import sys
from datetime import datetime

import yaml
from mysql.connector import connection
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re


units = [[1, 'kt', '48'],
          [7, 'D1 Lämmersalat', '38'],
          [8, 'D2 Verarmte Segge', '40'],
          [9, 'D3 Schwarzer Nachtschatten', '38'],
          [10, 'D4 Meeresleuchten', '38'],
          [11, 'D5 Kornrade', '38'],
          [12, 'D6 Wunder-Veilchen', '38'],
          [13, 'D7 Deutscher Ginster', '40'],
          [14, 'C1 Schwarze Teufelskralle', '39'],
          [15, 'C2 Großer Hufeisenklee', '39'],
          [16, 'C3 Zwerg Goldstern', '40'],
          [17, 'C4 Hibiskus', '40'],
          [18, 'C5 Sonnentau', '40'],
          [19, 'C6 Schöner Blaustern', '40'],
          [20, 'C7 Pyrenäen Drachenmaul', '35'],
          [21, 'C8 Flammen-Röschen', '39'],
          [22, 'A1 Blaue Himmesleiter', '37'],
          [23, 'A2 Türkenbund', '40'],
          [24, 'A3 Wassernuss', '39'],
          [25, 'A4 Goblin Gold', '39'],
          [26, 'A5 Preußisches Laserkraut', '40'],
          [27, 'A6 Aufgeblasener Fuchsschwanz', '40'],
          [28, 'A7 Teufelsauge', '38'],
          [29, 'A8 Feuer-Lilie', '38'],
          [30, 'E1 Knabenkraut', '39'],
          [31, 'E2 Sibirische Schwertlilie', '38'],
          [32, 'E3 Berg-Laserkraut', '37'],
          [33, 'E4 Kleine Seerose', '39'],
          [34, 'E5 Schachblume', '39'],
          [35, 'E6 Echte Bärentraube', '40'],
          [36, 'E7 Waldmeister', '40'],
          [37, 'B1 Lungenenzian', '40'],
          [38, 'B2 Schöner Lauch', '40'],
          [39, 'B3 Zwergteichrose', '40'],
          [40, 'B4 Nordischer Drachenkopf', '40'],
          [41, 'B5 Bunte Schwertlilie', '38'],
          [42, 'B6 Fingerhut', '40'],
          [43, 'B7 Sichelmöhre', '39'],
          [44, 'B8 Südlicher Wasserschlauch', '40'],
          [45, 'F1 Edelweiß', '40'],
          [46, 'F2 spreizender Storchschnabel', '40'],
          [47, 'F3 Trollblume', '40'],
          [48, 'F4 Wollige Wolfsmilch', '40'],
          [49, 'F5 Gewöhnliche Küchenschelle', '38'],
          [50, 'F6 Glanzloser Ehrenpreis', '39'],
          [51, 'F7 Adonis flammea', '40'],
          [52, 'F8 Unverwechselbarer Löwenzahn', '40'],
          [54, 'D8 Schlangenäuglein', '27'],
          [55, 'E8 Lothringer Lein', '37']]


def initial_token_request():
    url = 'https://register.2023wsjkorea.org/home/sub.php?menukey=1637&language=en'

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }

    response = requests.get(url, headers=headers)

    # Save cookies to variable
    cookie = response.cookies

    # Read token from response
    soup = BeautifulSoup(response.text, 'html.parser')
    #print(soup.prettify())
    token = soup.find('input', {'name': 'token'})['value']

    return cookie, token


def login_request(cookie, token, username, password):
    print("Login Request")
    url = 'https://register.2023wsjkorea.org/home/member.php'

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://register.2023wsjkorea.org',
        'Connection': 'keep-alive',
        'Referer': 'https://register.2023wsjkorea.org/home/sub.php?menukey=1637',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Site': 'same-origin',
        'Cookie': 'PHPSESSID=' + cookie.get('PHPSESSID')
    }

    data = {
        'mod': 'process',
        'act': 'mgsLogin',
        'rtnUrl': '',
        'token': token,
        'info1': username,
        'info2': password
    }

    response = requests.post(url, headers=headers, data=data)

    # Read token from response
    soup = BeautifulSoup(response.text, 'html.parser')
    # print(soup.prettify())

def navigate_to_unit_allocation(cookie, korea_unit_id, korea_patrol_id):
  print("Navigate to unit allocation")
  url = f"https://register.2023wsjkorea.org/home/sub.php?menukey=1706&mod=view1_manage_camp&code1={korea_unit_id}&code2={korea_patrol_id}"
  
  headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': 'https://register.2023wsjkorea.org/home/sub.php?menukey=1706',
    'Cookie': 'PHPSESSID=' + cookie.get('PHPSESSID')
    
  }

  response = requests.get(url, headers=headers)
  soup = BeautifulSoup(response.text, 'html.parser')
  # print(soup.prettify())
  token = soup.find('input', {'id': 'token'})['value']
  return token

def allocation(cookie, korea_unit_id, korea_patrol_id, korea_id): 
  print("Allocation")
  url = 'https://register.2023wsjkorea.org/base/php/wsj/apply_wsj_process.php'
  headers = {
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
      'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
      'Accept-Encoding': 'gzip, deflate, br',
      'Content-Type': 'application/x-www-form-urlencoded',
      'Origin': 'https://register.2023wsjkorea.org',
      'DNT': '1',
      'Connection': 'keep-alive',
      'Referer': 'https://register.2023wsjkorea.org/base/php/wsj/apply_wsj.php',
      'Cookie': 'PHPSESSID=' + cookie.get('PHPSESSID')
  }

  data = {
    'mod': 'process',
    'app_idcd': f"{korea_id}|",
    'cm_sub1': korea_unit_id,
    'cm_sub2': korea_patrol_id,
    'token': 'site_config_token'
  }

  response = requests.post(url, headers=headers, data=data)
  soup = BeautifulSoup(response.text, 'html.parser')
  # print(soup.prettify())
  # token = soup.find('input', {'id': 'token'})['value']
  match = re.search(r"alert\((.*)\);", soup.prettify())
  if match:
    alert_text = match.group(1)
    print(alert_text)
  else:
    print(soup.prettify())


def clear_allocation(cookie, token, korea_unit_id, korea_patrol_id,korea_id):
  print("Clear Allocation")
  url = f"https://register.2023wsjkorea.org/home/sub.php?menukey=1706&listCnt=20&code1={korea_unit_id}&code2={korea_patrol_id}"
  headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'de,en-US;q=0.7,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://register.2023wsjkorea.org',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': f"https://register.2023wsjkorea.org/home/sub.php?menukey=1706&mod=view1_manage_camp&code1={korea_unit_id}&code2={korea_patrol_id}",
    'Cookie': 'PHPSESSID=' + cookie.get('PHPSESSID')
  }

  data = {
    'mod': 'process',
    'act': 'sub_choice',
    'app_idcd': f"{korea_id}|",
    'group_status': 'C',
    'returnUrl': 'view1_manage_camp',
    'token': token 
  }

  response = requests.post(url, headers=headers, data=data)
  soup = BeautifulSoup(response.text, 'html.parser')
  # print(soup.prettify())



def allocate(cookie, korea_unit_id, dataframe):
   dataframe = dataframe.reset_index(drop=True)
   for index, row in dataframe.iterrows():
        korea_patrol_id = korea_unit_id + "-" + str( (index % 4 ) + 1) 
        print(f"{row['id']}, {row['role_wish']}, {row['first_name']}, {row['last_name']}")
        print(korea_unit_id, korea_patrol_id, row['korea_id'])
        # token = navigate_to_unit_allocation(cookie,  korea_unit_id, korea_patrol_id)
        # clear_allocation(cookie, token,  korea_unit_id, korea_patrol_id, row['korea_id'])
        allocation(cookie, korea_unit_id, korea_patrol_id, row['korea_id'])
        #print(token)

def main():
  print("=== Start Allocation ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

  with open("../config.yml", "r") as yamlfile:
      config = yaml.load(yamlfile, Loader=yaml.FullLoader)
      print("Read successful")
  
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
  df = pd.DataFrame(db_h)
  # print(dfh.columns)

  cookie, token = initial_token_request()
  login_request(cookie, token, config["korea_user"], config["korea_password"])
  
  for index, element in enumerate(units):
    korea_unit_id = "DE-" + str(index)
    primary_group_id = element[0]
    if index > 3:
      print(f"Allocate {korea_unit_id} - {element[1]} ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
      dfu = df.loc[df['primary_group_id'] == primary_group_id]
      dfuul = dfu.loc[df['role_wish'] == "Unit Leitung"]
      dfutn = dfu.loc[df['role_wish'] == "Teilnehmende*r"]

      print(f"{dfuul.shape[0]} Unit Leitungen")
      allocate(cookie, korea_unit_id, dfuul)

      print(f"{dfutn.shape[0]} Teilnehmende*r")
      allocate(cookie, korea_unit_id, dfutn)
    
  print("=== Finish Allocation ", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
   
if __name__ == "__main__":
    sys.exit(main())
