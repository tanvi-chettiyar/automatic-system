import sys,os
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from base.postgres_connector import PostgresConnection
from typing import Dict, List
from hashlib import md5

postgres_params = {
    'database': 'postgres',
    'host': 'localhost',
    'user': 'postgres',
    'password': 'myPassword',
    'port': '5432',}

def generate_hash(record_list: List[Dict], table_name: str) -> List[str]:
    for record in record_list:
        if table_name == 'locations':
            data_string = f"{record['name']},{record['region']},{record['country']},{record['lat']},{record['lon']},{record['tz_id']},{record['localtime_epoch']},{record['localtime']}"
            return md5(data_string.encode()).hexdigest()
        elif table_name == 'current':
            data_string = f"{record['last_updated_epoch']},{record['last_updated']},{record['temp_c']},{record['temp_f']},{record['is_day']},{record['wind_mph']},{record['wind_kph']},{record['wind_degree']},{record['wind_dir']},{record['pressure_mb']},{record['pressure_in']},{record['precip_mm']},{record['precip_in']},{record['humidity']},{record['cloud']},{record['feelslike_c']},{record['feelslike_f']},{record['windchill_c']},{record['windchill_f']},{record['heatindex_c']},{record['heatindex_f']},{record['dewpoint_c']},{record['dewpoint_f']},{record['vis_km']},{record['vis_miles']},{record['uv']},{record['gust_mph']},{record['gust_kph']},{record['condition.text']},{record['condition.icon']},{record['condition.code']}"
            return md5(data_string.encode()).hexdigest()
        elif table_name == 'daily':
            data_string = f"{record['date']}, {record['date_epoch']}, {record['maxtemp_c']}, {record['maxtemp_f']}, {record['mintemp_c']}, {record['mintemp_f']}, {record['avgtemp_c']}, {record['avgtemp_f']}, {record['maxwind_mph']}, {record['maxwind_kph']}, {record['totalprecip_mm']}, {record['totalprecip_in']}, {record['totalsnow_cm']}, {record['avgvis_km']}, {record['avgvis_miles']}, {record['avghumidity']}, {record['daily_will_it_rain']}, {record['daily_chance_of_rain']}, {record['daily_will_it_snow']}, {record['daily_chance_of_snow']}, {record['uv']}, {record['condition.text']}, {record['condition.icon']}, {record['condition.code']}"
            return md5(data_string.encode()).hexdigest()
        elif table_name == 'hourly':
            hash_list =[]
            data_string = f"{record['time_epoch']}, {record['time']}, {record['temp_c']}, {record['temp_f']}, {record['is_day']}, {record['wind_mph']}, {record['wind_kph']}, {record['wind_degree']}, {record['wind_dir']}, {record['pressure_mb']}, {record['pressure_in']}, {record['precip_mm']}, {record['precip_in']}, {record['snow_cm']}, {record['humidity']}, {record['cloud']}, {record['feelslike_c']}, {record['feelslike_f']}, {record['windchill_c']}, {record['windchill_f']}, {record['heatindex_c']}, {record['heatindex_f']}, {record['dewpoint_c']}, {record['dewpoint_f']}, {record['will_it_rain']}, {record['chance_of_rain']}, {record['will_it_snow']}, {record['chance_of_snow']}, {record['vis_km']}, {record['vis_miles']}, {record['gust_mph']}, {record['gust_kph']}, {record['uv']}, {record['condition.text']}, {record['condition.icon']}, {record['condition.code']}, {record['date']}, {record['date_epoch']}"
            data_hash = md5(data_string.encode()).hexdigest()
            hash_list = hash_list.append(data_hash)
            return hash_list
        else:
            raise ValueError("Table not found. Please select one of the following weather tables: locations, current, daily, or hourly")


def load_location_data(data: Dict, data_hash: str,csvfile: str):
    with PostgresConnection(**postgres_params) as [conn, cur]:
        cur.execute("""SELECT id, rec_hash FROM stage.locations WHERE latitude = %s AND longitude = %s;""", (data[0]["lat"], data[0]["lon"]))
        existing_record = cur.fetchone()

        if existing_record:
            existing_id, existing_hash = existing_record
            new_id = int(existing_id)+1
            if existing_hash != data_hash:
                with open(csvfile, mode='r') as data:
                    cur.copy_expert(sql="copy stage.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
                    cur.execute(f"update stage.locations set rec_hash = '{data_hash}' where id = {new_id}")

                    conn.commit()

                cur.execute(f"update prep.locations set is_active = False where id = {existing_id}")
                with open(csvfile, mode='r') as data:
                    cur.copy_expert(sql="copy prep.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
                    cur.execute(f"update prep.locations set last_modified = CURRENT_TIMESTAMP, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                    conn.commit()
                cur.execute(f"update target.locations set end_date = CURRENT_DATE, is_active = False where id = {existing_id}")
                with open(csvfile, mode='r') as data:
                    cur.copy_expert(sql="copy target.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
                    cur.execute(f"update target.locations set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                    conn.commit()
            else:
                pass
        
        else:
            cur.execute('select max(id) from stage.current;')
            existing_id = cur.fetchone()
            if not existing_id:
                new_id = 1
            else:
                new_id = int(existing_id)+1

            with open(csvfile, mode='r') as data:
                cur.copy_expert(sql="copy stage.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
                cur.execute(f"update stage.locations set rec_hash = '{data_hash}' where id = {new_id}")

                conn.commit()

            with open(csvfile, mode='r') as data:
                cur.copy_expert(sql="copy prep.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
                cur.execute(f"update prep.locations set last_modified = CURRENT_TIMESTAMP, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                conn.commit()

            with open(csvfile, mode='r') as data:
                cur.copy_expert(sql="copy target.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
                cur.execute(f"update target.locations set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                conn.commit()

def load_current_data(data_hash: str, csvfile: str):
    with PostgresConnection(**postgres_params) as [conn, cur]:
        cur.execute(f"""select id, rec_hash from prep.current where is_active = True;""")
        existing_record = cur.fetchone()

        if existing_record:
            existing_id, existing_hash = existing_record
            new_id = int(existing_id)+1
            if existing_hash != data_hash:
                with open(csvfile, mode='r') as data:
                    cur.copy_expert(sql="copy stage.current (last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                    cur.execute(f"update stage.current set rec_hash = '{data_hash}' where id = {new_id}")
                    conn.commit()

                cur.execute(f"update prep.current set is_active = False where id = {existing_id}")
                with open(csvfile, mode='r') as data:
                    cur.copy_expert(sql="copy prep.current (last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                    cur.execute(f"update prep.current set last_modified = CURRENT_TIMESTAMP, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                    conn.commit()

                cur.execute(f"update target.current set is_active = False where id = {existing_id}")
                with open(csvfile, mode='r') as data:
                    cur.copy_expert(sql="copy target.current (last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                    cur.execute(f"update target.current set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                    conn.commit()
            else:
                pass
        
        else:
            cur.execute('select max(id) from stage.current;')
            existing_id = cur.fetchone()
            if not existing_id:
                new_id = 1
            else:
                new_id = int(existing_id)+1

            with open(csvfile, mode='r') as data:
                cur.copy_expert(sql="copy stage.current (last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                cur.execute(f"update stage.current set rec_hash = '{data_hash}' where id = {new_id}")

                conn.commit()

            with open(csvfile, mode='r') as data:
                cur.copy_expert(sql="copy prep.current (last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                cur.execute(f"update prep.current set last_modified = CURRENT_TIMESTAMP, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                conn.commit()

            with open(csvfile, mode='r') as data:
                cur.copy_expert(sql="copy target.current (last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                cur.execute(f"update target.current set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                conn.commit()

def load_daily_data(data_hash: str, csvfile: str):
    with PostgresConnection(**postgres_params) as [conn, cur]:

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy stage.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)

            conn.commit()

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy prep.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f"update prep.daily set last_modified = CURRENT_TIMESTAMP, is_active = TRUE where id = {id};")

            conn.commit()

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy target.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f"update target.daily set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE where id = {id};")

            conn.commit()

def load_hourly_data(hourly_hash: List[str], csvfile: str, date: str):
    with PostgresConnection(**postgres_params) as [conn, cur]:
        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy stage.hourly (time_epoch, time, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv, condition_text, condition_icon, condition_code, date, date_epoch) from STDIN with csv header delimiter '|';", file=data)

            conn.commit()
            
        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy prep.hourly (time_epoch, time, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv, condition_text, condition_icon, condition_code, date, date_epoch) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f"update prep.hourly set last_modified = CURRENT_TIMESTAMP, is_active = TRUE where date = '{date}';")

            conn.commit()

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy target.hourly (time_epoch, time, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv, condition_text, condition_icon, condition_code, date, date_epoch) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f"update target.hourly set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE where date = '{date}';")

            conn.commit()



