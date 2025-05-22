#!/usr/bin/env python
import sys
from datetime import date

import yaml
from mysql.connector import connection
import pandas as pd
import requests
import os
from PIL import Image, ImageDraw, ImageFont


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

    db_people = pd.read_sql_query('''SELECT id, primary_group_id, first_name, last_name, korea_id, role_wish 
                                    FROM people 
                                    WHERE role_wish <> "" 
                                    AND status NOT IN ("abgemeldet", "Abmeldung Vermerkt", "in Überprüfung durch KT", "")''', cnx)
    df_people = pd.DataFrame(db_people)

    db_groups = pd.read_sql_query('''SELECT id, name FROM groups''', cnx)
    df_groups = pd.DataFrame(db_groups)
    df_groups = df_groups.rename(
        columns={'id': 'group_id', 'name': 'group_name'})

    df = pd.merge(df_people, df_groups, left_on='primary_group_id',
                  right_on='group_id', how='left')

    for index, row in df_groups.iterrows():
        print(f'ID: {row["group_id"]}, Name: {row["group_name"]}')
        dirname = f'{row["group_id"]}-{row["group_name"].replace(" ", "-")}'

        os.makedirs(dirname, exist_ok=True)

        dfu = df.loc[df['primary_group_id'] == row["group_id"]]

        for pindex, prow in dfu.iterrows():
            if len(prow["korea_id"]) == 9:
                filename = f'{prow["id"]}-{prow["first_name"]}-{prow["last_name"]}-{prow["korea_id"]}'.replace(
                    " ", "-")
                url = f'https://register.2023wsjkorea.org/base/php/wsj/Qrcode/QR.php?id={prow["korea_id"]}'
                print(filename)

                image = Image.open(requests.get(url, stream=True).raw)
                width, height = image.size
                new_width = width * 3
                new_height = height * 3
                image = image.resize((new_width, new_height))
                draw = ImageDraw.Draw(image)
                font = ImageFont.truetype('/Library/Fonts/Verdana.ttf', 10)
                draw.text(
                    (1, 1), f'{prow["id"]}: {prow["first_name"]} {prow["last_name"]}', font=font, fill='black')
                draw.text((1, 12), f'{prow["korea_id"]}',
                          font=font, fill='black')
                image.save(f'{dirname}/{filename}.png')


if __name__ == "__main__":
    sys.exit(main())
