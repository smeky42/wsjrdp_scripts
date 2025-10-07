from __future__ import annotations

import logging as _logging
import sys

import wsjrdp2027
import pandas as _pandas

import requests
from requests.structures import CaseInsensitiveDict
from urllib.parse import quote

_LOGGER = _logging.getLogger(__name__)

def main():
    start_time = None

    ctx = wsjrdp2027.WsjRdpContext(
        start_time=start_time,
        out_dir="data",
    )
    out_base = ctx.make_out_path("example_people_dataframe_{{ filename_suffix }}")
    ctx.configure_log_file(out_base.with_suffix(".log"))

    api_key= ctx.config.geo_api_key

    with ctx.psycopg_connect() as conn:
        df = wsjrdp2027.load_people_dataframe(conn, exclude_deregistered=False)

    _LOGGER.info("Found %s people", len(df))

    for index, row in df.iterrows(): # .iloc[500:1500]
        street = row['street'] if _pandas.notna(row['street']) else "No street"
        housenumber = row['housenumber'] if _pandas.notna(row['housenumber']) else ""
        town = row['town'] if _pandas.notna(row['town']) else ""
        zip_code = row['zip_code'] if _pandas.notna(row['zip_code']) else ""
        longitude = row['longitude'] if _pandas.notna(row['longitude']) else "0"
        latitude = row['latitude'] if _pandas.notna(row['latitude']) else "0"   
        country = row['country'] if _pandas.notna(row['country']) else "de"

        address = f"{street} {housenumber} {zip_code} {town} {country}"
        print(f"id: {row['id']} Address: {address} Coords: {longitude}, {latitude}")

        if (longitude == "0" or latitude == "0") and street != "No street":  
            encoded_address = quote(address)
            url = f"https://api.geoapify.com/v1/geocode/search?text={encoded_address}&format=json&apiKey={api_key}"

            headers = CaseInsensitiveDict()
            headers["Accept"] = "application/json"

            resp = requests.get(url, headers=headers)

            print(resp.status_code)
                    
            response = requests.get(url)
            print(response.json())
            response_json = response.json()

            longitude = response_json['results'][0]['lon']
            latitude = response_json['results'][0]['lat']
            country = response_json['results'][0]['country_code']

            print(f"id: {row['id']} Coords: {longitude}, {latitude} {country}")
            
            with ctx.psycopg_connect() as conn:
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE people
                            SET longitude = %s, latitude = %s 
                            WHERE id = %s
                        """, (longitude, latitude, row['id']))
                    
                    conn.commit()
                    print(f"id: {row['id']} Updated! {row['first_name']}, {row['last_name']}")
                
                except Exception as update_error:
                    conn.rollback()
                    print(f"id: {row['id']} Error updating coordinates for {row['first_name']} {row['last_name']}: {update_error}")
                    raise

if __name__ == "__main__":
    sys.exit(main())












