import requests
import psycopg2
from datetime import datetime
from typing import Final 
from json import dumps, load

# WeatherAPI Configuration
API_KEY: Final[str] = "c2fe5ebd74734989a33163320252302"
# BASE_URL: Final[str] = "http://api.weatherapi.com/v1/forecast.json?key=c2fe5ebd74734989a33163320252302&q=08854&days=1&aqi=no&alerts=no"
BASE_URL: Final[str] = "http://api.weatherapi.com/v1/forecast.json"
CITY: Final[str]= "New York"
COUNTRY: Final[str] = "US"


data_filepath = f"""/home/tanvi/Documents/GitRepo/automatic-system/etl/data/weather_{datetime.today().strftime("%Y%m%d")}.json"""
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
    
    # print(weather_data)

    weather_dict = {"today": {"temp_c": "23", "condition": "sunny"}, "tomorrow": {"temp_c": "24", "condition": "rainy"}}
    weather = {"today": [{"temp_c": "23", "condition": "sunny"},{"temp_c": "24", "condition": "rainy"}]}
    a = ['23', 'sunny']

    temp = {"temp_c": 23 }
    temp2 = {"temp_c": 24}
    b = {'temperature': [temp, temp2]}
    new = weather_dict["today"]["temp_c"]
    print(new)



    for item in weather["today"]:
        
        print(item['condition'])

    # print(weather_dict['condition'])

    from pandas import json_normalize

    df_simple = json_normalize(weather)
    print(df_simple.head())

    # store_weather_data(weather_data)
    print("Weather data stored successfully.")
