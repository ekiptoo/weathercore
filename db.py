import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "weatheruser"),
        password=os.getenv("DB_PASS"),
        database=os.getenv("DB_NAME", "weathercore"),
        autocommit=False
    )

def insert_observation(conn, station_id, data: dict):
        sql = """
              INSERT INTO observations
              (station_id, observed_at, temperature_c, humidity_pct,
               pressure_hpa, wind_speed_ms, wind_dir_deg,
               precipitation_mm, cloud_cover_pct, visibility_km,
               uv_index, weather_code, source_api, raw_json)
              VALUES (%(station_id)s, %(observed_at)s, %(temperature_c)s, \
                      %(humidity_pct)s, %(pressure_hpa)s, %(wind_speed_ms)s, \
                      %(wind_dir_deg)s, %(precipitation_mm)s, %(cloud_cover_pct)s, \
                      %(visibility_km)s, %(uv_index)s, %(weather_code)s, \
                      %(source_api)s, %(raw_json)s) ON DUPLICATE KEY \
              UPDATE temperature_c = \
              VALUES (temperature_c) \
              """

        cur = conn.cursor()
        cur.execute(sql, {"station_id": station_id, **data})
        conn.commit()