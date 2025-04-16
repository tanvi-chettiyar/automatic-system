truncate stage."location";
truncate stage.currently;
truncate stage.daily;
truncate stage.hourly;

with etl_table as(
select (locations->'name')::varchar(100) as name,
	(locations->'region')::varchar(50) as region,
	(locations->'country')::varchar(50) as country,
	(locations->'lat')::decimal(8,6) as latitude,
	(locations->'lon')::decimal(9,6) as longitude,
	(locations->'tz_id')::varchar(50) as timezone_id,
	(locations->'localtime_epoch')::bigint as localtime_epoch,
	(locations->'localtime')::varchar(50) as localtime
from
(select (wdata->>'location'):: jsonb as locations
from apublic.weather_json_etl))
insert into stage."location" (city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time)
select etl.* 
from etl_table etl;

update stage."location"
set 
    process_uuid = COALESCE(process_uuid , gen_random_uuid()::text),
    zipcode = COALESCE(zipcode, '08854')
where process_uuid is null or zipcode is null;

update stage."location"
set city_name = TRIM(both '"' from city_name),
	region = TRIM(both '"' from region),
	country = TRIM(both '"' from country),
	timezone_id = TRIM(both '"' from timezone_id),
	local_time = TRIM(both '"' from local_time)
where true;

with etl_table as (
	select (currently->'last_updated_epoch')::bigint as last_updated_epoch,
	(currently->>'last_updated')::timestamp as last_updated,
	(currently->'temp_c')::float as temp_c,
	(currently->'temp_f')::float as temp_f,
	(currently->'is_day')::float as is_day,
	(currently->'condition'->'text')::text as condition_text,
	(currently->'condition'->'icon')::text as condition_icon,
	(currently->'condition'->'code')::text as condition_code,
	(currently->'wind_mph')::float as wind_mph,
	(currently->'wind_kph')::float as wind_kph,
	(currently->'wind_degree')::float as wind_degree,
	(currently->'wind_dir')::text as wind_dir,
	(currently->'pressure_mb')::float as pressure_mb,
	(currently->'pressure_in')::float as pressure_in,
	(currently->'precip_mm')::float as precip_mm,
	(currently->'precip_in')::float as precip_in,
	(currently->'humidity')::float as humidity,
	(currently->'cloud')::float as cloud,
	(currently->'feelslike_c')::float as feelslike_c,
	(currently->'feelslike_f')::float as feelslike_f,
	(currently->'windchill_c')::float as windchill_c,
	(currently->'windchill_f')::float as windchill_f,
	(currently->'heatindex_c')::float as heatindex_c,
	(currently->'heatindex_f')::float as heatindex_f,
	(currently->'dewpoint_c')::float as dewpoint_c,
	(currently->'dewpoint_f')::float as dewpoint_f,
	(currently->'vis_km')::float as vis_km,
	(currently->'vis_miles')::float as vis_miles,
	(currently->'uv')::float as uv,
	(currently->'gust_mph')::float as gust_mph,
	(currently->'gust_kph')::float as gust_kph
	from
		(select (wdata->>'current')::jsonb as currently
		from apublic.weather_json_etl))
insert into stage.currently (last_updated_epoch, last_updated, temp_c, temp_f, is_day, condition_text, condition_icon, condition_code, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph)
select etl.*
from etl_table etl;

WITH puuid AS (
    SELECT process_uuid 
    FROM stage."location" 
    WHERE date(local_time::timestamp) = Current_date
    LIMIT 1
)
UPDATE stage.currently
SET process_uuid = (SELECT process_uuid FROM puuid)
WHERE process_uuid IS NULL;

update stage.currently 
set zipcode = '08854'
where zipcode is null;

update stage.currently
set wind_dir = TRIM(both '"' from wind_dir),
	condition_text = TRIM(both '"' from condition_text),
	condition_icon = Trim(both '"' from condition_icon)
where true;


with etl_table as (
	select (daily->'maxtemp_c')::float as maxtemp_c,
		(daily->'maxtemp_f')::float as maxtemp_f,
		(daily->'mintemp_c')::float as mintemp_c,
		(daily->'mintemp_f')::float as mintemp_f,
		(daily->'avgtemp_c')::float as avgtemp_c,
		(daily->'avgtemp_f')::float as avgtemp_f,
		(daily->'maxwind_mph')::float as maxwind_mph,
		(daily->'maxwind_kph')::float as maxwind_kph,
		(daily->'totalprecip_mm')::float as totalprecip_mm,
		(daily->'totalprecip_in')::float as totalprecip_in,
		(daily->'totalsnow_cm')::float as totalsnow_cm,
		(daily->'avgvis_km')::float as avgvis_km,
		(daily->'avgvis_miles')::float as avgvis_miles,
		(daily->'avghumidity')::float as avghumidity,
		(daily->'daily_will_it_rain')::float as daily_will_it_rain,
		(daily->'daily_chance_of_rain')::float as daily_chance_of_rain,
		(daily->'daily_will_it_snow')::float as daily_will_it_snow,
		(daily->'daily_chance_of_snow')::float as daily_chance_of_snow,
		(daily->'condition'->'text')::text as condition_text,
		(daily->'condition'->'icon')::text as condition_icon,
		(daily->'condition'->'code')::text as condition_code,
		(daily->'uv')::float as uv
	from
	(SELECT 
	    (forecastday->'day')::jsonb AS daily
	FROM apublic.weather_json_etl,
	LATERAL jsonb_array_elements(
	    (wdata->'forecast'->'forecastday')::jsonb
	) AS forecastday))
insert into stage.daily (maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, condition_text, condition_icon, condition_code, uv)
select etl.* 
from etl_table etl;

WITH puuid AS (
    SELECT process_uuid 
    FROM stage."location" 
    WHERE date(local_time::timestamp) = Current_Date
    LIMIT 1
)
UPDATE stage.daily
SET process_uuid = (SELECT process_uuid FROM puuid)
WHERE process_uuid IS NULL;

update stage.daily 
set zipcode = '08854'
where zipcode is null;

update stage.daily
set condition_text = TRIM(both '"' from condition_text),
	condition_icon = Trim(both '"' from condition_icon)
where true;

with etl_table as( 
select (hourly->'time_epoch')::bigint as time_epoch,
	(hourly->>'time')::timestamp as "time",
	(hourly->'temp_c')::float as temp_c,
	(hourly->'temp_f')::float as temp_f,
	(hourly->'is_day')::float as is_day,
	(hourly->'condition'->'text')::text as condition_text,
	(hourly->'condition'->'icon')::text as condition_icon,
	(hourly->'condition'->'code')::text as condition_code,
	(hourly->'wind_mph')::float as wind_mph,
	(hourly->'wind_kph')::float as wind_kph,
	(hourly->'wind_degree')::float as wind_degree,
	(hourly->'wind_dir')::text as wind_dir,
	(hourly->'pressure_mb')::float as pressure_mb,
	(hourly->'pressure_in')::float as pressure_in,
	(hourly->'precip_mm')::float as precip_mm,
	(hourly->'precip_in')::float as precip_in,
	(hourly->'snow_cm')::float as snow_cm,
	(hourly->'humidity')::float as humidity,
	(hourly->'cloud')::float as cloud,
	(hourly->'feelslike_c')::float as feelslike_c,
	(hourly->'feelslike_f')::float as feelslike_f,
	(hourly->'windchill_c')::float as windchill_c,
	(hourly->'windchill_f')::float as windchill_f,
	(hourly->'heatindex_c')::float as heatindex_c,
	(hourly->'heatindex_f')::float as heatindex_f,
	(hourly->'dewpoint_c')::float as dewpoint_c,
	(hourly->'dewpoint_f')::float as dewpoint_f,
	(hourly->'will_it_rain')::float as will_it_rain,
	(hourly->'chance_of_rain')::float as chance_of_rain,
	(hourly->'will_it_snow')::float as will_it_snow,
	(hourly->'chance_of_snow')::float as chance_of_snow,
	(hourly->'vis_km')::float as vis_km,
	(hourly->'vis_miles')::float as vis_miles,
	(hourly->'gust_mph')::float as gust_mph,
	(hourly->'gust_kph')::float as gust_kph,
	(hourly->'uv')::float as uv
from
(select
(forecasthour)::jsonb as hourly
from apublic.weather_json_etl wje,
lateral jsonb_array_elements(wdata->'forecast'->'forecastday') as forecastday,
lateral jsonb_array_elements(forecastday->'hour') as forecasthour))
insert into stage.hourly (time_epoch, "time", temp_c, temp_f, is_day, condition_text, condition_icon, condition_code, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv)
select etl.* 
from etl_table etl;

with etl_table as (
	SELECT 
	    (forecastday->>'date') AS hourlydate,
	    (forecastday->'date_epoch') as hourlyepoch
	FROM apublic.weather_json_etl,
	LATERAL jsonb_array_elements(wdata->'forecast'->'forecastday') AS forecastday)
update stage.hourly
set "date" = (etl_table.hourlydate)::date,
	date_epoch = (etl_table.hourlyepoch)::bigint
from etl_table
where stage.hourly."date" is null or stage.hourly.date_epoch is null;

WITH puuid AS (
    SELECT process_uuid 
    FROM stage."location" 
    WHERE date(local_time::timestamp) = Current_Date
    LIMIT 1
)
UPDATE stage.hourly
SET process_uuid = (SELECT process_uuid FROM puuid)
WHERE process_uuid IS NULL;

update stage.hourly 
set zipcode = '08854'
where zipcode is null;

update stage.hourly
set wind_dir = trim(both '"' from wind_dir),
	condition_text = TRIM(both '"' from condition_text),
	condition_icon = Trim(both '"' from condition_icon)
where true;
