import requests
import psycopg2
from datetime import datetime
from typing import Final, Any, List, Dict 
from json import dumps, load
from pandas import DataFrame as DF, json_normalize
import os
# import load_weather_data as lwd
# from load_weather_data import ( load_current_data, load_daily_data, load_hourly_data, load_location_data )
from load_weather_data import *


# WeatherAPI Configuration
API_KEY: Final[str] = "c2fe5ebd74734989a33163320252302"
# BASE_URL: Final[str] = "http://api.weatherapi.com/v1/forecast.json?key=c2fe5ebd74734989a33163320252302&q=08854&days=1&aqi=no&alerts=no"
BASE_URL: Final[str] = "http://api.weatherapi.com/v1/forecast.json"
CITY: Final[str]= "New York"
COUNTRY: Final[str] = "US"

#{datetime.today().strftime("%Y%m%d")}

base_path = os.path.dirname(os.path.realpath(__file__))
# data_filepath = os.path.join(base_path,f'''data/weather_{datetime.today().strftime("%Y%m%d")}.json''')
# data_csvpath1 = os.path.join(base_path,f'''data/weather_{datetime.today().strftime("%Y%m%d")}_1.csv''')
# data_csvpath2 = os.path.join(base_path,f'''data/weather_{datetime.today().strftime("%Y%m%d")}_2.csv''')
# data_csvpath3 = os.path.join(base_path,f'''data/weather_{datetime.today().strftime("%Y%m%d")}_3.csv''')
# data_csvpath4 = os.path.join(base_path,f'''data/weather_{datetime.today().strftime("%Y%m%d")}_4.csv''')

data_filepath = os.path.join(base_path,f'''data/weather_20250227.json''')
data_csvpath1 = os.path.join(base_path,f'''data/weather_20250227_1.csv''')
data_csvpath2 = os.path.join(base_path,f'''data/weather_20250227_2.csv''')
data_csvpath3 = os.path.join(base_path,f'''data/weather_20250227_3.csv''')
data_csvpath4 = os.path.join(base_path,f'''data/weather_20250227_4.csv''')

pull_request = False

# current_date = f'{datetime.today().strftime("%Y%m%d")}'
# current_date = "2025-02-27"

# Function to fetch weather data
def fetch_weather(city: str, country: str, pull_request: bool = False):
    if pull_request:
        # params = {"q": f"{city},{country}", "appid": API_KEY, "units": "metric"}
        params = {"key":API_KEY, "q": "08854", "days": "1", "aqi": "no", "alerts": "no"}
        response = requests.get(BASE_URL, params=params)
        return response.json()
    else:
        with open(data_filepath, "r") as data:
            return load(data)


#Function to store weather data into csv file and a dictionary        
def store_weather (weather_data, table_name: str, csv_path: str) -> Dict[str, Any]:
    if table_name == 'locations':
        location_data = weather_data['location']
        location_df = DF([location_data])
        location_df.to_csv(path_or_buf=csv_path, sep='|', header=True, mode='w', index=False)
        location_dict = location_df.to_dict(orient='records')
        return location_dict
    
    elif table_name =='current':
        current_data = weather_data['current']
        current_df = json_normalize(current_data)
        current_df.columns = current_df.columns.str.replace("current.", "", regex=False)
        current_df.to_csv(path_or_buf=csv_path, sep='|', header=True, mode='w', index=False)
        current_dict = current_df.to_dict(orient='records')
        return current_dict
    
    elif table_name == 'daily':
        daily_df = json_normalize(weather_data['forecast']['forecastday'][0]['day'])
        daily_df.insert(loc=0, column='date', value=weather_data['forecast']['forecastday'][0]['date'])
        daily_df.insert(loc=1, column='date_epoch', value=weather_data['forecast']['forecastday'][0]['date_epoch'])
        daily_df.to_csv(path_or_buf=csv_path, sep="|", header=True, mode='w', index=False)
        daily_dict = daily_df.to_dict(orient='records')
        return daily_dict
    
    elif table_name == 'hourly':
        hourly_df =  json_normalize(weather_data, 
                          record_path=['forecast', 'forecastday', 'hour'], 
                          meta=[['forecast', 'forecastday', 'date'], ['forecast', 'forecastday', 'date_epoch']], 
                          sep='.', 
                          max_level=None)
        hourly_df = hourly_df.rename(columns={'forecast.forecastday.date': 'date', 'forecast.forecastday.date_epoch':'date_epoch'})
        hourly_df.to_csv(path_or_buf=csv_path, sep='|', header=True, mode='w', index=False)
        hourly_dict = hourly_df.to_dict(orient='records')
        return hourly_dict


# Function to create hash for records 
def generate_hash(record_list: List[Dict], table_name: str) -> List[str]:
    hash_list =[]
    for record in record_list:
        if table_name == 'locations':
            data_string = f"{record['name']},{record['region']},{record['country']},{record['lat']},{record['lon']},{record['tz_id']},{record['localtime_epoch']},{record['localtime']}"
            print(data_string)
            data_hash = md5(data_string.encode()).hexdigest()
            hash_list.append(data_hash)

        elif table_name == 'current':
            data_string = f"{record['last_updated_epoch']},{record['last_updated']},{record['temp_c']},{record['temp_f']},{record['is_day']},{record['wind_mph']},{record['wind_kph']},{record['wind_degree']},{record['wind_dir']},{record['pressure_mb']},{record['pressure_in']},{record['precip_mm']},{record['precip_in']},{record['humidity']},{record['cloud']},{record['feelslike_c']},{record['feelslike_f']},{record['windchill_c']},{record['windchill_f']},{record['heatindex_c']},{record['heatindex_f']},{record['dewpoint_c']},{record['dewpoint_f']},{record['vis_km']},{record['vis_miles']},{record['uv']},{record['gust_mph']},{record['gust_kph']},{record['condition.text']},{record['condition.icon']},{record['condition.code']}"
            data_hash = md5(data_string.encode()).hexdigest()
            hash_list.append(data_hash)

        elif table_name == 'daily':
            data_string = f"{record['date']}, {record['date_epoch']}, {record['maxtemp_c']}, {record['maxtemp_f']}, {record['mintemp_c']}, {record['mintemp_f']}, {record['avgtemp_c']}, {record['avgtemp_f']}, {record['maxwind_mph']}, {record['maxwind_kph']}, {record['totalprecip_mm']}, {record['totalprecip_in']}, {record['totalsnow_cm']}, {record['avgvis_km']}, {record['avgvis_miles']}, {record['avghumidity']}, {record['daily_will_it_rain']}, {record['daily_chance_of_rain']}, {record['daily_will_it_snow']}, {record['daily_chance_of_snow']}, {record['uv']}, {record['condition.text']}, {record['condition.icon']}, {record['condition.code']}"
            data_hash = md5(data_string.encode()).hexdigest()
            hash_list.append(data_hash)

        elif table_name == 'hourly':
            data_string = f"{record['time_epoch']}, {record['time']}, {record['temp_c']}, {record['temp_f']}, {record['is_day']}, {record['wind_mph']}, {record['wind_kph']}, {record['wind_degree']}, {record['wind_dir']}, {record['pressure_mb']}, {record['pressure_in']}, {record['precip_mm']}, {record['precip_in']}, {record['snow_cm']}, {record['humidity']}, {record['cloud']}, {record['feelslike_c']}, {record['feelslike_f']}, {record['windchill_c']}, {record['windchill_f']}, {record['heatindex_c']}, {record['heatindex_f']}, {record['dewpoint_c']}, {record['dewpoint_f']}, {record['will_it_rain']}, {record['chance_of_rain']}, {record['will_it_snow']}, {record['chance_of_snow']}, {record['vis_km']}, {record['vis_miles']}, {record['gust_mph']}, {record['gust_kph']}, {record['uv']}, {record['condition.text']}, {record['condition.icon']}, {record['condition.code']}, {record['date']}, {record['date_epoch']}"
            data_hash = md5(data_string.encode()).hexdigest()
            hash_list.append(data_hash)

        else:
            raise ValueError("Table not found. Please select one of the following weather tables: locations, current, daily, or hourly")
        
    if len(hash_list) == 1:
        return hash_list[0]
    else:
        return hash_list



# Main Execution
if __name__ == "__main__":
    weather_data = fetch_weather(CITY, COUNTRY, pull_request)

    if pull_request:
        with open(data_filepath, "w") as writej:
            writej.write(dumps(weather_data, indent=2))

    location_dict = store_weather(weather_data, 'locations', data_csvpath1)
    location_hash = generate_hash(location_dict, 'locations')
    print(location_hash)

    current_dict = store_weather(weather_data, 'current', data_csvpath2)
    current_hash = generate_hash(current_dict, 'current')

    daily_dict = store_weather(weather_data, 'daily', data_csvpath3)
    daily_hash = generate_hash(daily_dict, 'daily')
    
    hourly_dict = store_weather(weather_data, 'hourly', data_csvpath4)
    hourly_hash = generate_hash(hourly_dict, 'hourly')


    # load_location_data(location_dict, location_hash, data_csvpath1)
    # load_current_data(current_hash, data_csvpath2)
    # load_daily_data(daily_hash, data_csvpath3)
    # load_hourly_data(hourly_hash, data_csvpath4, current_date)

    print('Weather data loaded successfully')




