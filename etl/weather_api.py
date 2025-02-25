import requests
import psycopg2
from datetime import datetime
from typing import Final 
from json import dumps, load
from pandas import DataFrame as DF, json_normalize
import os
import load_weather_data as lwd

# WeatherAPI Configuration
API_KEY: Final[str] = "c2fe5ebd74734989a33163320252302"
# BASE_URL: Final[str] = "http://api.weatherapi.com/v1/forecast.json?key=c2fe5ebd74734989a33163320252302&q=08854&days=1&aqi=no&alerts=no"
BASE_URL: Final[str] = "http://api.weatherapi.com/v1/forecast.json"
CITY: Final[str]= "New York"
COUNTRY: Final[str] = "US"

#{datetime.today().strftime("%Y%m%d")}

base_path = os.path.dirname(os.path.realpath(__file__))
data_filepath = os.path.join(base_path,f'''data/weather_20250223.json''')
data_csvpath1 = os.path.join(base_path,f'''data/weather_20250223_1.csv''')
data_csvpath2 = os.path.join(base_path,f'''data/weather_20250223_2.csv''')
data_csvpath3 = os.path.join(base_path,f'''data/weather_20250223_3.csv''')
data_csvpath4 = os.path.join(base_path,f'''data/weather_20250223_4.csv''')
pull_request = False

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


# Main Execution
if __name__ == "__main__":
    weather_data = fetch_weather(CITY, COUNTRY, pull_request)

    if pull_request:
        with open(data_filepath, "w") as writej:
            writej.write(dumps(weather_data, indent=2))

    location_data = weather_data['location']
    location_df = DF([location_data])
    location_df.to_csv(path_or_buf=data_csvpath1, sep='|', header=True, mode='w', index=False)
    
    current_data = weather_data['current']
    current_df = json_normalize(current_data)
    current_df.columns = current_df.columns.str.replace("current.", "", regex=False)
    current_df.to_csv(path_or_buf=data_csvpath2, sep='|', header=True, mode='w', index=False)

    # ndf3 =  json_normalize(weather_data, 
    #                       record_path=['forecast', 'forecastday', 'day'], 
    #                       meta=[['forecast', 'forecastday', 'date'], ['forecast', 'forecastday', 'date_epoch']], 
    #                       sep='.', 
    #                     #   errors='ignore', 
    #                       max_level=None)
    
    # print(ndf3)

    ndf4 =  json_normalize(weather_data, 
                          record_path=['forecast', 'forecastday', 'hour'], 
                          meta=[['forecast', 'forecastday', 'date'], ['forecast', 'forecastday', 'date_epoch'], ['forecast', 'forecastday', 'day', 'maxtemp_c'] ], 
                          sep='.', 
                        #   errors='ignore', 
                          max_level=None)

    # ndf4.to_csv(path_or_buf=data_csvpath, sep='|', header=True, mode='w', index=False)

    # store_weather_data(weather_data)
    print("Weather data stored successfully.")

    # lwd.load_location_data(data_csvpath1, 1)
