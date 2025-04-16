--select * from stage.location;
--
--select *, md5(concat(city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time)) rec_hash_sql 
--from stage.location ;
--
--UPDATE stage.location
--SET rec_hash = MD5(CONCAT(city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time))
--WHERE NULL;
--
--select * from stage.current;
--
--select *, md5(concat(last_updated_epoch,last_updated,temp_c,temp_f,is_day,wind_mph,wind_kph,wind_degree,wind_dir,pressure_mb,pressure_in,precip_mm,precip_in,humidity,cloud,feelslike_c,feelslike_f,windchill_c,windchill_f,heatindex_c,heatindex_f,dewpoint_c,dewpoint_f,vis_km,vis_miles,uv,gust_mph,gust_kph,condition_text,condition_icon,condition_code)) rec_hash 
--from stage.current ;
--
--select * from stage.daily;
--
--select *, md5(concat(maxtemp_c,maxtemp_f,mintemp_c,mintemp_f,avgtemp_c,avgtemp_f,maxwind_mph,maxwind_kph,totalprecip_mm,totalprecip_in,totalsnow_cm,avgvis_km,avgvis_miles,avghumidity,daily_will_it_rain,daily_chance_of_rain,daily_will_it_snow,daily_chance_of_snow,uv,condition_text,condition_icon,condition_code)) rec_hash 
--from stage.daily ;
--
--select * from stage.hourly;
--
--select *, md5(concat(time_epoch,"time",temp_c,temp_f,is_day,wind_mph,wind_kph,wind_degree,wind_dir,pressure_mb,pressure_in,precip_mm,precip_in,snow_cm,humidity,cloud,feelslike_c,feelslike_f,windchill_c,windchill_f,heatindex_c,heatindex_f,dewpoint_c,dewpoint_f,will_it_rain,chance_of_rain,will_it_snow,chance_of_snow,vis_km,vis_miles,gust_mph,gust_kph,uv,condition_text,condition_icon,condition_code,"date",date_epoch)) rec_hash 
--from stage.hourly ;
--
--update stage.location set md5(concat(city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time)) where True;
--
--alter table public.my_table add column rec_hash text;
--
--insert into select *, md5(concat("name",subject,grade)) rec_hash_sql from apublic.my_table;





