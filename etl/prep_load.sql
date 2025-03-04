--prep load
--for location table
truncate prep.location ;

with stage_table as (
select * , MD5(CONCAT(city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time)) sys_rec_hash
from stage.location 
)
insert into prep.location
select stg.*,
	case when ( target.latitude is null and target.longitude is null ) then 'I'
	     when stg.sys_rec_hash <> target.sys_rec_hash then 'U'
	     else 'N'
	end as sys_cdc_flag
from stage_table stg 
left join target.location target on ( stg.latitude = target.latitude and stg.longitude = target.longitude );

commit ;

-- select * from prep.location;

--for current table
truncate prep.currently;

with stage_table as (
select *, md5(concat(last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, process_uuid, condition_text, condition_icon, condition_code)) sys_rec_hash
from stage.currently
)
insert into prep.currently
select stg.*,
	case when (target.process_uuid is null) then 'I'
		 when stg.sys_rec_hash != target.sys_rec_hash then 'U'
		 else 'N'
	end as sys_cdc_flag
from stage_table stg
left join target.currently target on (stg.process_uuid = target.process_uuid);

commit;

-- select* from prep.currently;

--for daily table
truncate prep.daily;

with stage_table as (
select *, md5(concat(maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, process_uuid, condition_text, condition_icon, condition_code)) sys_rec_hash
from stage.daily
)
insert into prep.daily
select stg.*,
	case when (target.process_uuid is null) then 'I'
		 when stg.sys_rec_hash != target.sys_rec_hash then 'U'
		 else 'N'
	end as sys_cdc_flag
from stage_table stg
left join target.daily target on (stg.process_uuid = target.process_uuid);

commit;

-- select* from prep.daily;

--for hourly table

truncate prep.hourly;

with stage_table as (
select *, md5(concat(time_epoch, "time", temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv, condition_text, condition_icon, condition_code, "date", date_epoch, process_uuid)) sys_rec_hash
from stage.hourly
)
insert into prep.hourly 
select stg.*,
	case when (target.process_uuid is null) then 'I'
		 when stg.sys_rec_hash != target.sys_rec_hash then 'U'
		 else 'N'
	end as sys_cdc_flag
from stage_table stg
left join target.hourly target on (stg.process_uuid = target.process_uuid);

commit;

-- select * from prep.hourly;