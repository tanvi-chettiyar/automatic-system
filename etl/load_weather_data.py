import sys,os
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base.postgres_connector import PostgresConnection

postgres_params = {
    'database': 'postgres',
    'host': 'localhost',
    'user': 'postgres',
    'password': 'myPassword',
    'port': '5432',}

def load_location_data(csvfile: str, id: int):
    with PostgresConnection(**postgres_params) as [conn, cur]:
        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy stage.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)

            cur.copy_expert(sql="copy prep.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f'update prep.locations set last_updated = CURRENT_TIMESTAMP, is_active = TRUE where id = {id};')

            cur.copy_expert(sql="copy target.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f'update target.locations set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE where id = {id};')

            conn.commit()

def load_current_data(csvfile: str, id: int):
    pass