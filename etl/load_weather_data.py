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


def load_location_data(data: Dict, data_hash: str,csvfile: str):
    with PostgresConnection(**postgres_params) as [conn, cur]:
        with open(csvfile, mode='r') as data:
            cur.copy_expert(sql="copy stage.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
            
        cur.execute(f"""UPDATE stage.locations
                        SET rec_hash = MD5(CONCAT(city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time))
                        WHERE NULL""")
        cur.execute(f"""SELECT local_time, rec_hash FROM stage.locations WHERE latitude = {data[0]['lat']} AND longitude = {data[0]['lon']} and local_time LIKE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') || '%';""")
        stage_record = cur.fetchone()

        stage_time, stage_hash = stage_record

        cur.execute(f"""select rec_hash from prep.locations where local_time like to_char(CURRENT_DATE), """)
        # new_id = int(existing_id)-1
        # cur.execute
        # if existing_hash != data_hash:
            
                    
        #     cur.execute(f"update stage.locations set rec_hash = '{data_hash}' where id = {existing_id}")
        #     conn.commit()

            # cur.execute(f"update prep.locations set is_active = False where id = {existing_id}")
            # cur.execute(f"""update prep.locations as pl 
            #             set city_name = sl.city_name, 
            #                 region = sl.region, 
            #                 country = sl.country, 
            #                 latitude = sl.latitude, 
            #                 longitude = sl.longitude, 
            #                 timezone_id = sl.timezone_id, 
            #                 local_time_epoch = sl.local_time_epoch, 
            #                 local_time = sl.local_time 
            #             from stage.locations as sl 
            #             where pl.id = sl.id and pl.local_time LIKE TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') || '%'
            #             """)
            # cur.execute(f"update prep.locations set last_modified = CURRENT_TIMESTAMP, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")
            # conn.commit()

            # cur.execute(f"update target.locations set end_date = CURRENT_DATE, is_active = False where id = {existing_id}")
            # cur.execute(f"""update target.locations as tl 
            #             set city_name = pl.city_name, 
            #                 region = pl.region, 
            #                 country = pl.country, 
            #                 latitude = pl.latitude, 
            #                 longitude = pl.longitude, 
            #                 timezone_id = pl.timezone_id, 
            #                 local_time_epoch = pl.local_time_epoch, 
            #                 local_time = pl.local_time 
            #             from prep.locations as pl 
            #             where tl.id = pl.id
            #             """)
            # cur.execute(f"update target.locations set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")
            # conn.commit()

        # else:
        #     print("No Update to Locations table")
    
    # else:
    #     cur.execute('select max(id) from stage.current;')
    #     existing_id = cur.fetchone()
    #     if not existing_id:
    #         new_id = 1
    #     else:
    #         new_id = int(existing_id)+1

    #     with open(csvfile, mode='r') as data:
    #         cur.copy_expert(sql="copy stage.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time) from STDIN with csv header delimiter '|';", file=data)
    #         cur.execute(f"update stage.locations set rec_hash = '{data_hash}' where id = {new_id}")

    #         conn.commit()


    #         cur.execute(f"""insert into prep.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time)
    #                         select 
    #                             city_name = sl.city_name, 
    #                             region = sl.region, 
    #                             country = sl.country, 
    #                             latitude = sl.latitude, 
    #                             longitude = sl.longitude, 
    #                             timezone_id = sl.timezone_id, 
    #                             local_time_epoch = sl.local_time_epoch, 
    #                             local_time = sl.local_time 
    #                         from stage.locations sl 
    #                         where sl.id = {new_id}
    #                         """)
    #         cur.execute(f"update prep.locations set last_modified = CURRENT_TIMESTAMP, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

    #         conn.commit()


    #         cur.execute(f"""insert into target.locations (city_name, region, country, latitude, longitude, timezone_id, local_time_epoch, local_time)
    #                         select 
    #                             city_name = pl.city_name, 
    #                             region = pl.region, 
    #                             country = pl.country, 
    #                             latitude = pl.latitude, 
    #                             longitude = pl.longitude, 
    #                             timezone_id = pl.timezone_id, 
    #                             local_time_epoch = pl.local_time_epoch, 
    #                             local_time = pl.local_time 
    #                         from prep.locations pl 
    #                         where pl.id = {new_id}
    #                         """)
    #         cur.execute(f"update target.locations set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

    #         conn.commit()

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

                cur.execute(f"update target.current set end_date = CURRENT_DATE, is_active = False where id = {existing_id}")
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
        cur.execute(f"""select id, rec_hash from prep.daily where is_active = True;""")
        existing_record = cur.fetchone()

        if existing_record:
            existing_id, existing_hash = existing_record
            new_id = int(existing_id)+1

            if existing_hash != data_hash:
                with open(csvfile, mode='r') as data:
                    cur.copy_expert(sql="copy stage.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                    cur.execute(f"update stage.daily set rec_hash = '{data_hash}' where id = {new_id}")

                    conn.commit()

                cur.execute(f"update prep.daily set is_active = False where id = {existing_id}")
                with open(csvfile, mode='r') as data:
                    cur.copy_expert(sql="copy prep.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                    cur.execute(f"update prep.daily set last_modified = CURRENT_TIMESTAMP, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                    conn.commit()

                cur.execute(f"update target.daily set end_date = CURRENT_DATE, is_active = False where id = {existing_id}")
                with open(csvfile, mode='r') as data:
                    cur.copy_expert(sql="copy target.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                    cur.execute(f"update target.daily set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                    conn.commit()
            else:
                pass
        
        else:
            cur.execute('select max(id) from stage.daily;')
            existing_id = cur.fetchone()
            if not existing_id:
                new_id = 1
            else:
                new_id = int(existing_id)+1
            
            with open(csvfile, mode='r') as data:
                cur.copy_expert(sql="copy stage.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                cur.execute(f"update stage.daily set rec_hash = '{data_hash}' where id = {new_id}")

                conn.commit()

            with open(csvfile, mode='r') as data:
                cur.copy_expert(sql="copy prep.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                cur.execute(f"update prep.daily set last_modified = CURRENT_TIMESTAMP, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                conn.commit()
            
            with open(csvfile, mode='r') as data:
                cur.copy_expert(sql="copy target.daily (date, date_epoch, maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, condition_text, condition_icon, condition_code) from STDIN with csv header delimiter '|';", file=data)
                cur.execute(f"update target.daily set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE, rec_hash = '{data_hash}' where id = {new_id};")

                conn.commit()



# def load_hourly_data(hourly_hash: List[str], csvfile: str):
#     with PostgresConnection(**postgres_params) as [conn, cur]:
#         cur.execute(f"""select id, rec_hash from prep.hourly where is_active = True;""")
#         existing_record = cur.fetchall()
#         if existing_record:
#             new_id = 0  
#             for hash in hourly_hash:
#                 exists = any(hash in tup for tup in existing_record)
#                 new_id += 1
#                 if not exists:

        # with open(csvfile, mode='r') as data:
        #     cur.copy_expert(sql="copy stage.hourly (time_epoch, time, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv, condition_text, condition_icon, condition_code, date, date_epoch) from STDIN with csv header delimiter '|';", file=data)

        #     conn.commit()
            
        # with open(csvfile, mode='r') as data:
        #     cur.copy_expert(sql="copy prep.hourly (time_epoch, time, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv, condition_text, condition_icon, condition_code, date, date_epoch) from STDIN with csv header delimiter '|';", file=data)
        #     cur.execute(f"update prep.hourly set last_modified = CURRENT_TIMESTAMP, is_active = TRUE where date = '{date}';")

        #     conn.commit()

        # with open(csvfile, mode='r') as data:
        #     cur.copy_expert(sql="copy target.hourly (time_epoch, time, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv, condition_text, condition_icon, condition_code, date, date_epoch) from STDIN with csv header delimiter '|';", file=data)
        #     cur.execute(f"update target.hourly set start_date = CURRENT_DATE, end_date = NULL, is_active = TRUE where date = '{date}';")

        #     conn.commit()