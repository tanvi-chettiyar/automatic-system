update target.location tgl
set city_name = prl.city_name,
	region = prl.region,
	country = prl.country ,
	latitude = prl.latitude ,
	longitude = prl.longitude,
	timezone_id = prl.timezone_id,
	local_time_epoch = prl.local_time_epoch,
	local_time = prl.local_time,
	process_uuid = prl.process_uuid,
	zipcode = prl.zipcode,
	sys_rec_hash = prl.sys_rec_hash 
from prep.location prl	
where prl.sys_cdc_flag = 'U';
update target.location tgl
set sys_end_date = Current_Date,
	sys_is_active = false
from prep.location prl
where prl.sys_cdc_flag = 'I' and tgl.sys_end_date is Null;
insert into target.location
select city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time,process_uuid,zipcode,sys_rec_hash
from prep.location
where sys_cdc_flag = 'I';
update target.location
set sys_start_date = Current_Date,
	sys_is_active = True
where sys_start_date is Null;
commit ;
update target.currently tgc
set last_updated_epoch = prc.last_updated_epoch,
	last_updated = prc.last_updated,
	temp_c = prc.temp_c,
	temp_f = prc.temp_f,
	is_day = prc.is_day,
	wind_mph = prc.wind_mph,
	wind_kph = prc.wind_kph,
	wind_degree = prc.wind_degree,
	wind_dir = prc.wind_dir,
	pressure_mb = prc.pressure_mb,
	pressure_in= prc.pressure_in,
	precip_mm = prc.precip_mm,
	precip_in = prc.precip_in,
	humidity = prc.humidity,
	cloud = prc.cloud,
	feelslike_c = prc.feelslike_c,
	feelslike_f = prc.feelslike_f,
	windchill_c = prc.windchill_c,
	windchill_f = prc.windchill_f,
	heatindex_c = prc.heatindex_c,
	heatindex_f = prc.heatindex_f,
	dewpoint_c = prc.dewpoint_c,
	dewpoint_f = prc.dewpoint_f,
	vis_km = prc.vis_km,
	vis_miles = prc.vis_miles,
	uv = prc.uv,
	gust_mph = prc.gust_mph,
	gust_kph = prc.gust_kph,
	process_uuid = prc.process_uuid,
	zipcode = prc.zipcode,
	condition_text = prc.condition_text,
	condition_icon = prc.condition_icon,
	condition_code = prc.condition_code,
	sys_rec_hash = prc.sys_rec_hash
from prep.currently prc
where prc.sys_cdc_flag = 'U';
update target.currently tgc
set sys_end_date = Current_Date,
	sys_is_active = False
from prep.currently prc
where prc.sys_cdc_flag = 'I' and tgc.sys_end_date is Null;
insert into target.currently
select last_updated_epoch, last_updated, temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, vis_km, vis_miles, uv, gust_mph, gust_kph, process_uuid, zipcode, condition_text, condition_icon, condition_code, sys_rec_hash
from prep.currently 
where sys_cdc_flag = 'I';
update target.currently
set sys_start_date = Current_Date,
	sys_is_active = True
where sys_start_date is Null;
commit;
update target.daily tgd
set maxtemp_c = prd.maxtemp_c,
	maxtemp_f = prd.maxtemp_f,
	mintemp_c = prd.mintemp_c,
	mintemp_f = prd.mintemp_f,
	avgtemp_c = prd.avgtemp_c,
	avgtemp_f = prd.avgtemp_f,
	maxwind_mph = prd.maxwind_mph,
	maxwind_kph = prd.maxwind_kph,
	totalprecip_mm = prd.totalprecip_mm,
	totalprecip_in = prd.totalprecip_in,
	totalsnow_cm = prd.totalsnow_cm,
	avgvis_km = prd.avgvis_km,
	avgvis_miles = prd.avgvis_miles,
	avghumidity = prd.avghumidity,
	daily_will_it_rain = prd.daily_will_it_rain,
	daily_chance_of_rain = prd.daily_chance_of_rain,
	daily_will_it_snow = prd.daily_will_it_snow,
	daily_chance_of_snow = prd.daily_chance_of_snow,
	uv = prd.uv,
	process_uuid = prd.process_uuid,
	zipcode = prd.zipcode,
	condition_text = prd.condition_text,
	condition_icon = prd.condition_icon,
	condition_code = prd.condition_code,
	sys_rec_hash = prd.sys_rec_hash
from prep.daily prd
where prd.sys_cdc_flag ='U';
update target.daily tgd
set sys_end_date = Current_Date,
	sys_is_active = false
from prep.daily prd
where prd.sys_cdc_flag = 'I' and tgd.sys_end_date is Null;
insert into target.daily 
select maxtemp_c, maxtemp_f, mintemp_c, mintemp_f, avgtemp_c, avgtemp_f, maxwind_mph, maxwind_kph, totalprecip_mm, totalprecip_in, totalsnow_cm, avgvis_km, avgvis_miles, avghumidity, daily_will_it_rain, daily_chance_of_rain, daily_will_it_snow, daily_chance_of_snow, uv, process_uuid, zipcode, condition_text, condition_icon, condition_code, sys_rec_hash
from prep.daily
where sys_cdc_flag = 'I';
update target.daily
set sys_start_date = Current_Date,
	sys_is_active = true
where sys_start_date is null;
commit;
update target.hourly tgh
set time_epoch = prh.time_epoch,
	time = prh.time,
	temp_c = prh.temp_c,
	temp_f = prh.temp_f,
	is_day = prh.is_day,
	wind_mph = prh.wind_mph,
	wind_kph = prh.wind_kph,
	wind_degree = prh.wind_degree,
	wind_dir = prh.wind_dir,
	pressure_mb = prh.pressure_mb,
	pressure_in = prh.pressure_in,
	precip_mm = prh.precip_mm,
	precip_in = prh.precip_in,
	snow_cm = prh.snow_cm,
	humidity = prh.humidity,
	cloud = prh.cloud,
	feelslike_c = prh.feelslike_c,
	feelslike_f = prh.feelslike_f,
	windchill_c = prh.windchill_c,
	windchill_f = prh.windchill_f,
	heatindex_c = prh.heatindex_c,
	heatindex_f = prh.heatindex_f,
	dewpoint_c = prh.dewpoint_c,
	dewpoint_f = prh.dewpoint_f,
	will_it_rain = prh.will_it_rain,
	chance_of_rain = prh.chance_of_rain,
	will_it_snow = prh.will_it_snow,
	chance_of_snow = prh.chance_of_snow,
	vis_km = prh.vis_km,
	vis_miles = prh.vis_miles,
	gust_mph = prh.gust_mph,
	gust_kph = prh.gust_kph,
	uv = prh.uv,
	condition_text = prh.condition_text,
	condition_icon = prh.condition_icon,
	condition_code = prh.condition_code,
	"date" = prh.date,
	date_epoch = prh.date_epoch,
	process_uuid = prh.process_uuid,
	aipcode = prh.zipcode,
	sys_rec_hash = prh.sys_rec_hash
from prep.hourly prh
where prh.sys_cdc_flag  = 'U';
update target.hourly tgh
set sys_end_date = Current_Date,
	sys_is_active = false
from prep.hourly prh
where prh.sys_cdc_flag = 'I' and tgh.sys_end_date is Null;
insert into target.hourly
select time_epoch, "time", temp_c, temp_f, is_day, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, snow_cm, humidity, cloud, feelslike_c, feelslike_f, windchill_c, windchill_f, heatindex_c, heatindex_f, dewpoint_c, dewpoint_f, will_it_rain, chance_of_rain, will_it_snow, chance_of_snow, vis_km, vis_miles, gust_mph, gust_kph, uv, condition_text, condition_icon, condition_code, "date", date_epoch, process_uuid, zipcode, sys_rec_hash
from prep.hourly 
where sys_cdc_flag = 'I';
update target.hourly
set sys_start_date = Current_Date,
	sys_is_active = true
where sys_start_date is null;
commit;