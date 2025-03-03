
process_config = {
    "datasets": [
        { 
            "dataset_name" : "locations",
            "stage_sql": "", 
            "prep_sql": r"""
                truncate prep.location ;

                with stage_table as (
                select * , MD5(CONCAT(city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time)) rec_hash
                from stage.location 
                )
                insert into prep.location
                select stg.*,
                    case when target.latitude is null then 'I'
                        when stg.rec_hash <> target.rec_hash then 'U'
                        else 'N'
                    end as cdc_flag
                from stage_table stg 
                left join target.location target on ( stg.latitude = target.latitude and stg.longitude = target.longitude );
                """, 
            "target_sql": r"""
                update target.location tgt
                set city_name = perp.city_name,
                    region = perp.region,
                    country = perp.country ,
                    latitude = perp.latitude ,
                    longitude = perp.longitude,
                    timezone_id = perp.timezone_id,
                    local_time_epoch = perp.local_time_epoch,
                    local_time = perp.local_time,
                    process_uuid = perp.process_uuid,
                    rec_hash = perp.rec_hash 
                from prep.location perp	
                where perp.cdc_flag = 'U' ;

                insert into target.location
                select city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time,process_uuid,rec_hash
                from prep.location
                where cdc_flag = 'I' ;
                """
        },
        {
            "dataset_name" : "current",
            "stage_sql": "", 
            "prep_sql": "", 
            "target_sql": ""
        },
        {
            "dataset_name" : "daily",
            "stage_sql": "", 
            "prep_sql": "", 
            "target_sql": ""
        },
        {
            "dataset_name" : "hourly",
            "stage_sql": "", 
            "prep_sql": "", 
            "target_sql": ""
        },
    ]
}


from textwrap import dedent

for dataset in process_config["datasets"]:
    print(dataset["dataset_name"])
    print(dedent(dataset["stage_sql"]))
    print(dedent(dataset["prep_sql"]))
    print(dedent(dataset["target_sql"]))





# import pandas as pd
# import json

# # Sample JSON data (Replace this with your actual data source)
# data = {
#     "location": {
#         "name": "Piscataway",
#         "region": "New Jersey",
#         "country": "USA",
#         "xxx": "aaa",
#         "lat": 40.5527,
#         "lon": -74.4582,
#         "tz_id": "America/New_York",
#         "localtime_epoch": 1740332071,
#         "localtime": "2025-02-23 12:34"
#     },
#     "current": {
#         "last_updated_epoch": 1740331800,
#         "last_updated": "2025-02-23 12:30",
#         "temp_c": 6.7,
#         "temp_f": 44.1
#     }
# }

# # Use pandas json_normalize to flatten 'location' data
# # df_location = pd.json_normalize(data, record_path=None, meta=['location'])

# # # Extract only relevant location fields
# df_location = pd.json_normalize(data['location'])

# # Display result
# print(df_location)



# import pandas as pd

# # Sample JSON (Replace with your actual data)
# data = {
#     "forecast": {
#         "forecastday": [
#             {
#                 "date": "2025-02-23",
#                 "date_epoch": 1740268800,
#                 "day": {
#                     "maxtemp_c": 8.3,
#                     "maxtemp_f": 46.9,
#                     "mintemp_c": -2.9,
#                     "mintemp_f": 26.8,
#                     "avgtemp_c": 1.5,
#                     "avgtemp_f": 34.7,
#                     "maxwind_mph": 9.2,
#                     "maxwind_kph": 14.8,
#                     "totalprecip_mm": 0.0,
#                     "totalprecip_in": 0.0,
#                     "totalsnow_cm": 0.0,
#                     "avgvis_km": 10.0,
#                     "avgvis_miles": 6.0,
#                     "avghumidity": 65,
#                     "daily_will_it_rain": 0,
#                     "daily_chance_of_rain": 0,
#                     "daily_will_it_snow": 0,
#                     "daily_chance_of_snow": 0,
#                     "condition": {
#                         "text": "Partly Cloudy",
#                         "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png",
#                         "code": 1003
#                     },
#                     "uv": 0.7
#                 }
#             }
#         ]
#     }
# }

# # Flatten "day" data from the "forecastday" list
# df_day = pd.json_normalize(data['forecast']['forecastday'], meta=['date', 'date_epoch'], record_path=None)

# # Extract only "day" fields
# df_day = pd.json_normalize(data['forecast']['forecastday'], meta=['date', 'date_epoch'], record_path=None)

# # Expand "condition" nested dictionary
# # df_day = pd.concat([df_day.drop(columns=['day.condition']), df_day['day.condition'].apply(pd.Series).add_prefix('condition_')], axis=1)

# # Display result
# print(df_day)





# import pandas as pd

# # Sample DataFrame
# data = {
#     "name": ["Alice", "Bob", "Charlie"],
#     "age": [25, 30, 35],
#     "city": ["New York", "Los Angeles", "Chicago"],
#     "salary": [50000, 60000, 70000]
# }

# df = pd.DataFrame(data)

# # Exclude 'age' and 'salary' columns
# df_filtered = df.drop(columns=['age', 'salary'])

# print(df_filtered)

