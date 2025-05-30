create table apublic.weather_json (wdata jsonb);

truncate table apublic.weather_json;

INSERT INTO apublic.weather_json VALUES
(
'{
    "city": "New York",
    "metrics": {
        "temperature": 25,
        "humidity": 55,
        "wind_speed": 10
    },
    "forecasts": [
        {"date": "2025-03-12", "temperature": 15, "humidity": 60, "description": "äaaa"},
        {"date": "2025-03-13", "temperature": 17, "humidity": 65, "description": "a"}
    ]
}'
);

select * from apublic.weather_json;

show server_encoding;


SELECT wdata->>'city' AS city,
	wdata->>'metrics' as metrics,
--	metric.temperature as temperature,
	jsonb_extract_path(jsonb_extract_path(wdata, 'metrics'), 'temperature') AS temperature,
	jsonb_extract_path(jsonb_extract_path(wdata, 'metrics'), 'humidity') AS humidity,	
	jsonb_extract_path(jsonb_extract_path(wdata, 'metrics'), 'wind_speed') AS wind_speed,	
    forecast->>'date' AS date,
    (forecast->>'temperature')::int AS temperature,
    (forecast->>'humidity')::int AS humidity,
    (forecast->>'description') AS description
FROM apublic.weather_json,
LATERAL jsonb_array_elements(wdata->'forecasts') AS forecast
--LATERAL jsonb_each(wdata->'metrics') AS metrics
;


SELECT  
    wdata->>'city' AS city, 
    metric.key AS metric_name, 
    metric.value AS metric_value
FROM apublic.weather_json,
LATERAL jsonb_each(wdata->'metrics') AS metric;


select * from apublic.weather_json;

truncate table apublic.weather_json;

INSERT INTO apublic.weather_json VALUES
('{
    "city": "New York",
    "forecasts": [
        {"date": "2025-03-12", "temperature": 15, "humidity": 60, "description": "äaaa"},
        {"date": "2025-03-13", "temperature": 17, "humidity": 65, "description": "a"}
    ]
}');

rollback;

select * from apublic.weather_json;



insert into apublic.delete values ('aa') ;

--create table apublic.delete (wdata text);
--
--commit;

truncate apublic.delete ;

--insert into apublic.delete values ('aa') ;

--commit;


rollback;

select * from apublic.delete ;





begin transaction;

lock apublic.delete IN EXCLUSIVE mode ;

select pg_sleep(120);

truncate apublic.delete ;

insert into apublic.delete values ('aa') ;

rollback;

select * from apublic.delete ;

end transaction;



select * from pg_catalog.pg_locks pl ;



create table apublic.weather_json_etl  (wdata jsonb);

truncate apublic.weather_json_etl;

INSERT INTO apublic.weather_json_etl VALUES
(
'{ "location": { "name": "Piscataway", "region": "New Jersey", "country": "USA", "lat": 40.5527, "lon": -74.4582, "tz_id": "America/New_York", "localtime_epoch": 1740348364, "localtime": "2025-02-23 17:06" }, "current": { "last_updated_epoch": 1740348000, "last_updated": "2025-02-23 17:00", "temp_c": 7.8, "temp_f": 46.0, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 6.0, "wind_kph": 9.7, "wind_degree": 258, "wind_dir": "WSW", "pressure_mb": 1017.0, "pressure_in": 30.04, "precip_mm": 0.0, "precip_in": 0.0, "humidity": 37, "cloud": 0, "feelslike_c": 6.1, "feelslike_f": 42.9, "windchill_c": 2.4, "windchill_f": 36.3, "heatindex_c": 5.2, "heatindex_f": 41.4, "dewpoint_c": -3.9, "dewpoint_f": 25.1, "vis_km": 16.0, "vis_miles": 9.0, "uv": 0.0, "gust_mph": 10.1, "gust_kph": 16.3 }, "forecast": { "forecastday": [ { "date": "2025-02-23", "date_epoch": 1740268800, "day": { "maxtemp_c": 8.3, "maxtemp_f": 46.9, "mintemp_c": -2.9, "mintemp_f": 26.8, "avgtemp_c": 1.5, "avgtemp_f": 34.7, "maxwind_mph": 9.2, "maxwind_kph": 14.8, "totalprecip_mm": 0.0, "totalprecip_in": 0.0, "totalsnow_cm": 0.0, "avgvis_km": 10.0, "avgvis_miles": 6.0, "avghumidity": 65, "daily_will_it_rain": 0, "daily_chance_of_rain": 0, "daily_will_it_snow": 0, "daily_chance_of_snow": 0, "condition": { "text": "Partly Cloudy ", "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png", "code": 1003 }, "uv": 0.7 }, "astro": { "sunrise": "06:40 AM", "sunset": "05:43 PM", "moonrise": "03:59 AM", "moonset": "12:37 PM", "moon_phase": "Waning Crescent", "moon_illumination": 28, "is_moon_up": 0, "is_sun_up": 0 }, "hour": [ { "time_epoch": 1740286800, "time": "2025-02-23 00:00", "temp_c": -1.7, "temp_f": 29.0, "is_day": 0, "condition": { "text": "Clear ", "icon": "//cdn.weatherapi.com/weather/64x64/night/113.png", "code": 1000 }, "wind_mph": 7.4, "wind_kph": 11.9, "wind_degree": 239, "wind_dir": "WSW", "pressure_mb": 1022.0, "pressure_in": 30.19, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 73, "cloud": 13, "feelslike_c": -6.5, "feelslike_f": 20.3, "windchill_c": -6.5, "windchill_f": 20.3, "heatindex_c": -1.7, "heatindex_f": 29.0, "dewpoint_c": -6.0, "dewpoint_f": 21.2, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 12.8, "gust_kph": 20.6, "uv": 0 }, { "time_epoch": 1740290400, "time": "2025-02-23 01:00", "temp_c": -2.1, "temp_f": 28.3, "is_day": 0, "condition": { "text": "Partly Cloudy ", "icon": "//cdn.weatherapi.com/weather/64x64/night/116.png", "code": 1003 }, "wind_mph": 7.2, "wind_kph": 11.5, "wind_degree": 243, "wind_dir": "WSW", "pressure_mb": 1022.0, "pressure_in": 30.17, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 75, "cloud": 44, "feelslike_c": -6.8, "feelslike_f": 19.7, "windchill_c": -6.8, "windchill_f": 19.7, "heatindex_c": -2.1, "heatindex_f": 28.3, "dewpoint_c": -5.9, "dewpoint_f": 21.3, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 12.3, "gust_kph": 19.7, "uv": 0 }, { "time_epoch": 1740294000, "time": "2025-02-23 02:00", "temp_c": -2.4, "temp_f": 27.6, "is_day": 0, "condition": { "text": "Partly Cloudy ", "icon": "//cdn.weatherapi.com/weather/64x64/night/116.png", "code": 1003 }, "wind_mph": 6.9, "wind_kph": 11.2, "wind_degree": 244, "wind_dir": "WSW", "pressure_mb": 1021.0, "pressure_in": 30.16, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 78, "cloud": 25, "feelslike_c": -7.0, "feelslike_f": 19.3, "windchill_c": -7.0, "windchill_f": 19.3, "heatindex_c": -2.4, "heatindex_f": 27.6, "dewpoint_c": -5.9, "dewpoint_f": 21.4, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.9, "gust_kph": 19.1, "uv": 0 }, { "time_epoch": 1740297600, "time": "2025-02-23 03:00", "temp_c": -2.7, "temp_f": 27.2, "is_day": 0, "condition": { "text": "Clear ", "icon": "//cdn.weatherapi.com/weather/64x64/night/113.png", "code": 1000 }, "wind_mph": 6.9, "wind_kph": 11.2, "wind_degree": 242, "wind_dir": "WSW", "pressure_mb": 1021.0, "pressure_in": 30.15, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 80, "cloud": 8, "feelslike_c": -7.2, "feelslike_f": 19.1, "windchill_c": -7.2, "windchill_f": 19.1, "heatindex_c": -2.7, "heatindex_f": 27.2, "dewpoint_c": -5.8, "dewpoint_f": 21.6, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.7, "gust_kph": 18.8, "uv": 0 }, { "time_epoch": 1740301200, "time": "2025-02-23 04:00", "temp_c": -2.5, "temp_f": 27.5, "is_day": 0, "condition": { "text": "Partly Cloudy ", "icon": "//cdn.weatherapi.com/weather/64x64/night/116.png", "code": 1003 }, "wind_mph": 7.2, "wind_kph": 11.5, "wind_degree": 249, "wind_dir": "WSW", "pressure_mb": 1020.0, "pressure_in": 30.13, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 82, "cloud": 40, "feelslike_c": -6.9, "feelslike_f": 19.6, "windchill_c": -6.9, "windchill_f": 19.6, "heatindex_c": -2.5, "heatindex_f": 27.5, "dewpoint_c": -5.6, "dewpoint_f": 21.9, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.7, "gust_kph": 18.8, "uv": 0 }, { "time_epoch": 1740304800, "time": "2025-02-23 05:00", "temp_c": -2.4, "temp_f": 27.6, "is_day": 0, "condition": { "text": "Clear ", "icon": "//cdn.weatherapi.com/weather/64x64/night/113.png", "code": 1000 }, "wind_mph": 7.4, "wind_kph": 11.9, "wind_degree": 253, "wind_dir": "WSW", "pressure_mb": 1021.0, "pressure_in": 30.15, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 76, "cloud": 8, "feelslike_c": -6.7, "feelslike_f": 19.9, "windchill_c": -6.7, "windchill_f": 19.9, "heatindex_c": -2.4, "heatindex_f": 27.6, "dewpoint_c": -5.9, "dewpoint_f": 21.5, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 12.2, "gust_kph": 19.7, "uv": 0 }, { "time_epoch": 1740308400, "time": "2025-02-23 06:00", "temp_c": -2.4, "temp_f": 27.6, "is_day": 0, "condition": { "text": "Clear ", "icon": "//cdn.weatherapi.com/weather/64x64/night/113.png", "code": 1000 }, "wind_mph": 7.4, "wind_kph": 11.9, "wind_degree": 261, "wind_dir": "W", "pressure_mb": 1020.0, "pressure_in": 30.13, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 78, "cloud": 7, "feelslike_c": -6.6, "feelslike_f": 20.2, "windchill_c": -6.6, "windchill_f": 20.2, "heatindex_c": -2.4, "heatindex_f": 27.6, "dewpoint_c": -5.6, "dewpoint_f": 21.9, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.9, "gust_kph": 19.2, "uv": 0 }, { "time_epoch": 1740312000, "time": "2025-02-23 07:00", "temp_c": -2.4, "temp_f": 27.8, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 6.9, "wind_kph": 11.2, "wind_degree": 260, "wind_dir": "W", "pressure_mb": 1020.0, "pressure_in": 30.13, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 79, "cloud": 10, "feelslike_c": -6.4, "feelslike_f": 20.5, "windchill_c": -6.4, "windchill_f": 20.5, "heatindex_c": -2.4, "heatindex_f": 27.8, "dewpoint_c": -5.6, "dewpoint_f": 22.0, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.0, "gust_kph": 17.7, "uv": 0.0 }, { "time_epoch": 1740315600, "time": "2025-02-23 08:00", "temp_c": -1.9, "temp_f": 28.5, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 6.9, "wind_kph": 11.2, "wind_degree": 260, "wind_dir": "W", "pressure_mb": 1021.0, "pressure_in": 30.14, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 77, "cloud": 1, "feelslike_c": -5.8, "feelslike_f": 21.6, "windchill_c": -5.8, "windchill_f": 21.6, "heatindex_c": -1.9, "heatindex_f": 28.5, "dewpoint_c": -5.8, "dewpoint_f": 21.6, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 10.8, "gust_kph": 17.3, "uv": 0.5 }, { "time_epoch": 1740319200, "time": "2025-02-23 09:00", "temp_c": -0.6, "temp_f": 31.0, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 7.6, "wind_kph": 12.2, "wind_degree": 266, "wind_dir": "W", "pressure_mb": 1021.0, "pressure_in": 30.15, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 71, "cloud": 0, "feelslike_c": -4.3, "feelslike_f": 24.3, "windchill_c": -4.3, "windchill_f": 24.3, "heatindex_c": -0.6, "heatindex_f": 31.0, "dewpoint_c": -6.1, "dewpoint_f": 21.0, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 10.5, "gust_kph": 16.9, "uv": 1.2 }, { "time_epoch": 1740322800, "time": "2025-02-23 10:00", "temp_c": 1.0, "temp_f": 33.7, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 8.9, "wind_kph": 14.4, "wind_degree": 283, "wind_dir": "WNW", "pressure_mb": 1021.0, "pressure_in": 30.15, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 59, "cloud": 10, "feelslike_c": -2.6, "feelslike_f": 27.4, "windchill_c": -2.6, "windchill_f": 27.4, "heatindex_c": 1.0, "heatindex_f": 33.7, "dewpoint_c": -6.2, "dewpoint_f": 20.9, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.5, "gust_kph": 18.5, "uv": 2.2 }, { "time_epoch": 1740326400, "time": "2025-02-23 11:00", "temp_c": 2.2, "temp_f": 36.0, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 8.9, "wind_kph": 14.4, "wind_degree": 285, "wind_dir": "WNW", "pressure_mb": 1021.0, "pressure_in": 30.14, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 54, "cloud": 0, "feelslike_c": -1.1, "feelslike_f": 30.0, "windchill_c": -1.1, "windchill_f": 30.0, "heatindex_c": 2.2, "heatindex_f": 36.0, "dewpoint_c": -5.8, "dewpoint_f": 21.6, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.0, "gust_kph": 17.7, "uv": 2.9 }, { "time_epoch": 1740330000, "time": "2025-02-23 12:00", "temp_c": 3.3, "temp_f": 38.0, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 9.2, "wind_kph": 14.8, "wind_degree": 283, "wind_dir": "WNW", "pressure_mb": 1020.0, "pressure_in": 30.13, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 51, "cloud": 0, "feelslike_c": 0.1, "feelslike_f": 32.3, "windchill_c": 0.1, "windchill_f": 32.3, "heatindex_c": 3.3, "heatindex_f": 38.0, "dewpoint_c": -5.6, "dewpoint_f": 21.9, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.1, "gust_kph": 17.9, "uv": 3.2 }, { "time_epoch": 1740333600, "time": "2025-02-23 13:00", "temp_c": 5.6, "temp_f": 42.0, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 9.2, "wind_kph": 14.8, "wind_degree": 282, "wind_dir": "WNW", "pressure_mb": 1020.0, "pressure_in": 30.11, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 49, "cloud": 0, "feelslike_c": 2.7, "feelslike_f": 36.9, "windchill_c": 2.7, "windchill_f": 36.9, "heatindex_c": 5.6, "heatindex_f": 42.0, "dewpoint_c": -5.2, "dewpoint_f": 22.6, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.3, "gust_kph": 18.1, "uv": 2.7 }, { "time_epoch": 1740337200, "time": "2025-02-23 14:00", "temp_c": 6.9, "temp_f": 44.5, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 8.7, "wind_kph": 14.0, "wind_degree": 279, "wind_dir": "W", "pressure_mb": 1019.0, "pressure_in": 30.1, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 49, "cloud": 0, "feelslike_c": 4.2, "feelslike_f": 39.6, "windchill_c": 4.2, "windchill_f": 39.6, "heatindex_c": 6.9, "heatindex_f": 44.5, "dewpoint_c": -2.3, "dewpoint_f": 27.8, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.0, "gust_kph": 17.7, "uv": 1.8 }, { "time_epoch": 1740340800, "time": "2025-02-23 15:00", "temp_c": 6.5, "temp_f": 43.7, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 8.3, "wind_kph": 13.3, "wind_degree": 280, "wind_dir": "W", "pressure_mb": 1019.0, "pressure_in": 30.1, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 49, "cloud": 0, "feelslike_c": 3.7, "feelslike_f": 38.7, "windchill_c": 3.7, "windchill_f": 38.7, "heatindex_c": 6.5, "heatindex_f": 43.7, "dewpoint_c": -1.9, "dewpoint_f": 28.6, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 11.0, "gust_kph": 17.7, "uv": 0.9 }, { "time_epoch": 1740344400, "time": "2025-02-23 16:00", "temp_c": 5.9, "temp_f": 42.5, "is_day": 1, "condition": { "text": "Cloudy ", "icon": "//cdn.weatherapi.com/weather/64x64/day/119.png", "code": 1006 }, "wind_mph": 6.3, "wind_kph": 10.1, "wind_degree": 269, "wind_dir": "W", "pressure_mb": 1019.0, "pressure_in": 30.09, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 49, "cloud": 65, "feelslike_c": 3.0, "feelslike_f": 37.4, "windchill_c": 3.0, "windchill_f": 37.4, "heatindex_c": 5.9, "heatindex_f": 42.5, "dewpoint_c": -3.7, "dewpoint_f": 25.3, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 9.5, "gust_kph": 15.3, "uv": 0.4 }, { "time_epoch": 1740348000, "time": "2025-02-23 17:00", "temp_c": 7.8, "temp_f": 46.0, "is_day": 1, "condition": { "text": "Sunny", "icon": "//cdn.weatherapi.com/weather/64x64/day/113.png", "code": 1000 }, "wind_mph": 6.0, "wind_kph": 9.7, "wind_degree": 258, "wind_dir": "WSW", "pressure_mb": 1017.0, "pressure_in": 30.04, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 37, "cloud": 0, "feelslike_c": 2.4, "feelslike_f": 36.3, "windchill_c": 2.4, "windchill_f": 36.3, "heatindex_c": 5.2, "heatindex_f": 41.4, "dewpoint_c": -3.9, "dewpoint_f": 25.1, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 16.0, "vis_miles": 9.0, "gust_mph": 10.1, "gust_kph": 16.3, "uv": 0.0 }, { "time_epoch": 1740351600, "time": "2025-02-23 18:00", "temp_c": 4.6, "temp_f": 40.2, "is_day": 0, "condition": { "text": "Overcast ", "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "code": 1009 }, "wind_mph": 5.6, "wind_kph": 9.0, "wind_degree": 257, "wind_dir": "WSW", "pressure_mb": 1019.0, "pressure_in": 30.09, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 57, "cloud": 100, "feelslike_c": 2.0, "feelslike_f": 35.6, "windchill_c": 2.0, "windchill_f": 35.6, "heatindex_c": 4.6, "heatindex_f": 40.2, "dewpoint_c": -3.2, "dewpoint_f": 26.3, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 10.7, "gust_kph": 17.2, "uv": 0 }, { "time_epoch": 1740355200, "time": "2025-02-23 19:00", "temp_c": 4.0, "temp_f": 39.2, "is_day": 0, "condition": { "text": "Overcast ", "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "code": 1009 }, "wind_mph": 4.9, "wind_kph": 7.9, "wind_degree": 269, "wind_dir": "W", "pressure_mb": 1020.0, "pressure_in": 30.11, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 63, "cloud": 100, "feelslike_c": 1.7, "feelslike_f": 35.1, "windchill_c": 1.7, "windchill_f": 35.1, "heatindex_c": 4.0, "heatindex_f": 39.2, "dewpoint_c": -2.4, "dewpoint_f": 27.7, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 10.3, "gust_kph": 16.6, "uv": 0 }, { "time_epoch": 1740358800, "time": "2025-02-23 20:00", "temp_c": 3.6, "temp_f": 38.5, "is_day": 0, "condition": { "text": "Overcast ", "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "code": 1009 }, "wind_mph": 4.5, "wind_kph": 7.2, "wind_degree": 282, "wind_dir": "WNW", "pressure_mb": 1020.0, "pressure_in": 30.11, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 65, "cloud": 100, "feelslike_c": 1.6, "feelslike_f": 34.9, "windchill_c": 1.6, "windchill_f": 34.9, "heatindex_c": 3.6, "heatindex_f": 38.5, "dewpoint_c": -2.5, "dewpoint_f": 27.6, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 9.4, "gust_kph": 15.1, "uv": 0 }, { "time_epoch": 1740362400, "time": "2025-02-23 21:00", "temp_c": 3.2, "temp_f": 37.8, "is_day": 0, "condition": { "text": "Overcast ", "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "code": 1009 }, "wind_mph": 3.8, "wind_kph": 6.1, "wind_degree": 276, "wind_dir": "W", "pressure_mb": 1020.0, "pressure_in": 30.11, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 65, "cloud": 100, "feelslike_c": 1.3, "feelslike_f": 34.3, "windchill_c": 1.3, "windchill_f": 34.3, "heatindex_c": 3.2, "heatindex_f": 37.8, "dewpoint_c": -2.7, "dewpoint_f": 27.1, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 7.8, "gust_kph": 12.6, "uv": 0 }, { "time_epoch": 1740366000, "time": "2025-02-23 22:00", "temp_c": 2.8, "temp_f": 37.0, "is_day": 0, "condition": { "text": "Overcast ", "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "code": 1009 }, "wind_mph": 3.8, "wind_kph": 6.1, "wind_degree": 265, "wind_dir": "W", "pressure_mb": 1020.0, "pressure_in": 30.12, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 66, "cloud": 99, "feelslike_c": 0.7, "feelslike_f": 33.3, "windchill_c": 0.7, "windchill_f": 33.3, "heatindex_c": 2.8, "heatindex_f": 37.0, "dewpoint_c": -2.9, "dewpoint_f": 26.7, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 7.6, "gust_kph": 12.2, "uv": 0 }, { "time_epoch": 1740369600, "time": "2025-02-23 23:00", "temp_c": 2.4, "temp_f": 36.3, "is_day": 0, "condition": { "text": "Overcast ", "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "code": 1009 }, "wind_mph": 3.1, "wind_kph": 5.0, "wind_degree": 277, "wind_dir": "W", "pressure_mb": 1020.0, "pressure_in": 30.13, "precip_mm": 0.0, "precip_in": 0.0, "snow_cm": 0.0, "humidity": 67, "cloud": 100, "feelslike_c": 0.3, "feelslike_f": 32.5, "windchill_c": 0.3, "windchill_f": 32.5, "heatindex_c": 2.4, "heatindex_f": 36.3, "dewpoint_c": -3.0, "dewpoint_f": 26.5, "will_it_rain": 0, "chance_of_rain": 0, "will_it_snow": 0, "chance_of_snow": 0, "vis_km": 10.0, "vis_miles": 6.0, "gust_mph": 6.2, "gust_kph": 9.9, "uv": 0 } ] } ] } }'
);


select * from apublic.weather_json_etl;


{"lat": 40.5527, "lon": -74.4582, "name": "Piscataway", "tz_id": "America/New_York", "region": "New Jersey", "country": "USA", "localtime": "2025-02-23 17:06", "localtime_epoch": 1740348364}

select jsonb_extract_path(locations, 'name') as name
,jsonb_extract_path(locations, 'region') as region
,jsonb_extract_path(locations, 'country') as country
,jsonb_extract_path(locations, 'lat') as lat
,jsonb_extract_path(locations, 'lon') as lon
,jsonb_extract_path(locations, 'tz_id') as tz_id
,jsonb_extract_path(locations, 'localtime_epoch') as localtime_epoch
,jsonb_extract_path(locations, 'localtime') as localtime
,hours->>'time_epoch' AS time_epoch
from 
(
select (wdata->>'location')::jsonb as locations
--	, wdata->>'current'
--	, wdata->>'forecast'
--	,jsonb_extract_path(jsonb_extract_path(wdata, 'forecast'), 'forecastday') AS forecastday
--	,forecastday->>'day' as day
	,forecastday::jsonb as forecastday
from apublic.weather_json_etl
,LATERAL jsonb_array_elements(jsonb_extract_path(jsonb_extract_path(wdata, 'forecast'), 'forecastday')) AS forecastday
--,LATERAL jsonb_array_elements(jsonb_extract_path(jsonb_extract_path(jsonb_extract_path(wdata, 'forecast'), 'forecastday'), 'hour')) AS hours
)
,LATERAL jsonb_array_elements(forecastday->'hour') AS hours
;

jsonb_extract_path(jsonb_extract_path(jsonb_extract_path(wdata, 'forecast'), 'forecastday'),'hour')




[{"uv": 0, "time": "2025-02-23 00:00", "cloud": 13, "is_day": 0, "temp_c": -1.7, "temp_f": 29.0, "vis_km": 10.0, "snow_cm": 0.0, "gust_kph": 20.6, "gust_mph": 12.8, "humidity": 73, "wind_dir": "WSW", "wind_kph": 11.9, "wind_mph": 7.4, "condition": {"code": 1000, "icon": "//cdn.weatherapi.com/weather/64x64/night/113.png", "text": "Clear "}, "precip_in": 0.0, "precip_mm": 0.0, "vis_miles": 6.0, "dewpoint_c": -6.0, "dewpoint_f": 21.2, "time_epoch": 1740286800, "feelslike_c": -6.5, "feelslike_f": 20.3, "heatindex_c": -1.7, "heatindex_f": 29.0, "pressure_in": 30.19, "pressure_mb": 1022.0, "wind_degree": 239, "windchill_c": -6.5, "windchill_f": 20.3, "will_it_rain": 0, "will_it_snow": 0, "chance_of_rain": 0, "chance_of_snow": 0}, {"uv": 0, "time": "2025-02-23 23:00", "cloud": 100, "is_day": 0, "temp_c": 2.4, "temp_f": 36.3, "vis_km": 10.0, "snow_cm": 0.0, "gust_kph": 9.9, "gust_mph": 6.2, "humidity": 67, "wind_dir": "W", "wind_kph": 5.0, "wind_mph": 3.1, "condition": {"code": 1009, "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "text": "Overcast "}, "precip_in": 0.0, "precip_mm": 0.0, "vis_miles": 6.0, "dewpoint_c": -3.0, "dewpoint_f": 26.5, "time_epoch": 1740369600, "feelslike_c": 0.3, "feelslike_f": 32.5, "heatindex_c": 2.4, "heatindex_f": 36.3, "pressure_in": 30.13, "pressure_mb": 1020.0, "wind_degree": 277, "windchill_c": 0.3, "windchill_f": 32.5, "will_it_rain": 0, "will_it_snow": 0, "chance_of_rain": 0, "chance_of_snow": 0}]

{"day": {"uv": 0.7, "avgtemp_c": 1.5, "avgtemp_f": 34.7, "avgvis_km": 10.0, "condition": {"code": 1003, "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png", "text": "Partly Cloudy "}, "maxtemp_c": 8.3, "maxtemp_f": 46.9, "mintemp_c": -2.9, "mintemp_f": 26.8, "avghumidity": 65, "maxwind_kph": 14.8, "maxwind_mph": 9.2, "avgvis_miles": 6.0, "totalsnow_cm": 0.0, "totalprecip_in": 0.0, "totalprecip_mm": 0.0, "daily_will_it_rain": 0, "daily_will_it_snow": 0, "daily_chance_of_rain": 0, "daily_chance_of_snow": 0}, "date": "2025-02-23", "hour": [{"uv": 0, "time": "2025-02-23 00:00", "cloud": 13, "is_day": 0, "temp_c": -1.7, "temp_f": 29.0, "vis_km": 10.0, "snow_cm": 0.0, "gust_kph": 20.6, "gust_mph": 12.8, "humidity": 73, "wind_dir": "WSW", "wind_kph": 11.9, "wind_mph": 7.4, "condition": {"code": 1000, "icon": "//cdn.weatherapi.com/weather/64x64/night/113.png", "text": "Clear "}, "precip_in": 0.0, "precip_mm": 0.0, "vis_miles": 6.0, "dewpoint_c": -6.0, "dewpoint_f": 21.2, "time_epoch": 1740286800, "feelslike_c": -6.5, "feelslike_f": 20.3, "heatindex_c": -1.7, "heatindex_f": 29.0, "pressure_in": 30.19, "pressure_mb": 1022.0, "wind_degree": 239, "windchill_c": -6.5, "windchill_f": 20.3, "will_it_rain": 0, "will_it_snow": 0, "chance_of_rain": 0, "chance_of_snow": 0}, {"uv": 0, "time": "2025-02-23 23:00", "cloud": 100, "is_day": 0, "temp_c": 2.4, "temp_f": 36.3, "vis_km": 10.0, "snow_cm": 0.0, "gust_kph": 9.9, "gust_mph": 6.2, "humidity": 67, "wind_dir": "W", "wind_kph": 5.0, "wind_mph": 3.1, "condition": {"code": 1009, "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "text": "Overcast "}, "precip_in": 0.0, "precip_mm": 0.0, "vis_miles": 6.0, "dewpoint_c": -3.0, "dewpoint_f": 26.5, "time_epoch": 1740369600, "feelslike_c": 0.3, "feelslike_f": 32.5, "heatindex_c": 2.4, "heatindex_f": 36.3, "pressure_in": 30.13, "pressure_mb": 1020.0, "wind_degree": 277, "windchill_c": 0.3, "windchill_f": 32.5, "will_it_rain": 0, "will_it_snow": 0, "chance_of_rain": 0, "chance_of_snow": 0}], "date_epoch": 1740268800}


[{"day": {"uv": 0.7, "avgtemp_c": 1.5, "avgtemp_f": 34.7, "avgvis_km": 10.0, "condition": {"code": 1003, "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png", "text": "Partly Cloudy "}, "maxtemp_c": 8.3, "maxtemp_f": 46.9, "mintemp_c": -2.9, "mintemp_f": 26.8, "avghumidity": 65, "maxwind_kph": 14.8, "maxwind_mph": 9.2, "avgvis_miles": 6.0, "totalsnow_cm": 0.0, "totalprecip_in": 0.0, "totalprecip_mm": 0.0, "daily_will_it_rain": 0, "daily_will_it_snow": 0, "daily_chance_of_rain": 0, "daily_chance_of_snow": 0}, "date": "2025-02-23", "hour": [{"uv": 0, "time": "2025-02-23 00:00", "cloud": 13, "is_day": 0, "temp_c": -1.7, "temp_f": 29.0, "vis_km": 10.0, "snow_cm": 0.0, "gust_kph": 20.6, "gust_mph": 12.8, "humidity": 73, "wind_dir": "WSW", "wind_kph": 11.9, "wind_mph": 7.4, "condition": {"code": 1000, "icon": "//cdn.weatherapi.com/weather/64x64/night/113.png", "text": "Clear "}, "precip_in": 0.0, "precip_mm": 0.0, "vis_miles": 6.0, "dewpoint_c": -6.0, "dewpoint_f": 21.2, "time_epoch": 1740286800, "feelslike_c": -6.5, "feelslike_f": 20.3, "heatindex_c": -1.7, "heatindex_f": 29.0, "pressure_in": 30.19, "pressure_mb": 1022.0, "wind_degree": 239, "windchill_c": -6.5, "windchill_f": 20.3, "will_it_rain": 0, "will_it_snow": 0, "chance_of_rain": 0, "chance_of_snow": 0}, {"uv": 0, "time": "2025-02-23 23:00", "cloud": 100, "is_day": 0, "temp_c": 2.4, "temp_f": 36.3, "vis_km": 10.0, "snow_cm": 0.0, "gust_kph": 9.9, "gust_mph": 6.2, "humidity": 67, "wind_dir": "W", "wind_kph": 5.0, "wind_mph": 3.1, "condition": {"code": 1009, "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "text": "Overcast "}, "precip_in": 0.0, "precip_mm": 0.0, "vis_miles": 6.0, "dewpoint_c": -3.0, "dewpoint_f": 26.5, "time_epoch": 1740369600, "feelslike_c": 0.3, "feelslike_f": 32.5, "heatindex_c": 2.4, "heatindex_f": 36.3, "pressure_in": 30.13, "pressure_mb": 1020.0, "wind_degree": 277, "windchill_c": 0.3, "windchill_f": 32.5, "will_it_rain": 0, "will_it_snow": 0, "chance_of_rain": 0, "chance_of_snow": 0}], "date_epoch": 1740268800}]

{"forecastday": [{"day": {"uv": 0.7, "avgtemp_c": 1.5, "avgtemp_f": 34.7, "avgvis_km": 10.0, "condition": {"code": 1003, "icon": "//cdn.weatherapi.com/weather/64x64/day/116.png", "text": "Partly Cloudy "}, "maxtemp_c": 8.3, "maxtemp_f": 46.9, "mintemp_c": -2.9, "mintemp_f": 26.8, "avghumidity": 65, "maxwind_kph": 14.8, "maxwind_mph": 9.2, "avgvis_miles": 6.0, "totalsnow_cm": 0.0, "totalprecip_in": 0.0, "totalprecip_mm": 0.0, "daily_will_it_rain": 0, "daily_will_it_snow": 0, "daily_chance_of_rain": 0, "daily_chance_of_snow": 0}, "date": "2025-02-23", "hour": [{"uv": 0, "time": "2025-02-23 00:00", "cloud": 13, "is_day": 0, "temp_c": -1.7, "temp_f": 29.0, "vis_km": 10.0, "snow_cm": 0.0, "gust_kph": 20.6, "gust_mph": 12.8, "humidity": 73, "wind_dir": "WSW", "wind_kph": 11.9, "wind_mph": 7.4, "condition": {"code": 1000, "icon": "//cdn.weatherapi.com/weather/64x64/night/113.png", "text": "Clear "}, "precip_in": 0.0, "precip_mm": 0.0, "vis_miles": 6.0, "dewpoint_c": -6.0, "dewpoint_f": 21.2, "time_epoch": 1740286800, "feelslike_c": -6.5, "feelslike_f": 20.3, "heatindex_c": -1.7, "heatindex_f": 29.0, "pressure_in": 30.19, "pressure_mb": 1022.0, "wind_degree": 239, "windchill_c": -6.5, "windchill_f": 20.3, "will_it_rain": 0, "will_it_snow": 0, "chance_of_rain": 0, "chance_of_snow": 0}, {"uv": 0, "time": "2025-02-23 23:00", "cloud": 100, "is_day": 0, "temp_c": 2.4, "temp_f": 36.3, "vis_km": 10.0, "snow_cm": 0.0, "gust_kph": 9.9, "gust_mph": 6.2, "humidity": 67, "wind_dir": "W", "wind_kph": 5.0, "wind_mph": 3.1, "condition": {"code": 1009, "icon": "//cdn.weatherapi.com/weather/64x64/night/122.png", "text": "Overcast "}, "precip_in": 0.0, "precip_mm": 0.0, "vis_miles": 6.0, "dewpoint_c": -3.0, "dewpoint_f": 26.5, "time_epoch": 1740369600, "feelslike_c": 0.3, "feelslike_f": 32.5, "heatindex_c": 2.4, "heatindex_f": 36.3, "pressure_in": 30.13, "pressure_mb": 1020.0, "wind_degree": 277, "windchill_c": 0.3, "windchill_f": 32.5, "will_it_rain": 0, "will_it_snow": 0, "chance_of_rain": 0, "chance_of_snow": 0}], "date_epoch": 1740268800}]}



