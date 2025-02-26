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

            conn.commit()

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy prep.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f'update prep.locations set last_modified = CURRENT_TIMESTAMP, is_active = TRUE where id = {id};')

            conn.commit()

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy target.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f'update target.locations set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE where id = {id};')

            conn.commit()

def load_current_data(csvfile: str, id: int):

    with PostgresConnection(**postgres_params) as [conn, cur]:
        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy stage.current (last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)

            conn.commit()

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy prep.current (last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f'update prep.current set last_modified = CURRENT_TIMESTAMP, is_active = TRUE where id = {id};')

            conn.commit()

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy target.current (last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f'update target.current set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE where id = {id};')

            conn.commit()

def load_daily_data(csvfile: str, id: int):

    with PostgresConnection(**postgres_params) as [conn, cur]:
        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy stage.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)

            conn.commit()

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy prep.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f'update prep.daily set last_modified = CURRENT_TIMESTAMP, is_active = TRUE where id = {id};')

            conn.commit()

        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy target.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
            cur.execute(f'update target.daily set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE where id = {id};')

            conn.commit()

def load_hourly_data(csvfile: str, date: str):
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
