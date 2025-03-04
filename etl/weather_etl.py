import requests
import sys, os
sys.path.insert(0,(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from datetime import datetime
from typing import Final 
from json import load, dumps
from uuid import uuid4

from pathlib import Path
from helper.utils import *



process_uuid = str(uuid4())
stage_schema = 'stage'

base_file_path = Path(__file__).parent.absolute().joinpath("data", "weather")
json_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"weather_{datetime.today().strftime('%Y%m%d')}.json")

location_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"location_{datetime.today().strftime('%Y%m%d')}.csv")
currently_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"currently_{datetime.today().strftime('%Y%m%d')}.csv")
hourly_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"hourly_{datetime.today().strftime('%Y%m%d')}.csv")
daily_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"daily_{datetime.today().strftime('%Y%m%d')}.csv")

prep_load_script = Path(__file__).parent.absolute().joinpath("prep_load.sql")
target_load_script = Path(__file__).parent.absolute().joinpath("target_load.sql")

# json_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"weather_20250303.json")

# location_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"location_20250303.csv")
# currently_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"currently_20250303.csv")
# hourly_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"hourly_20250303.csv")
# daily_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", f"daily_20250303.csv")


# WeatherAPI Configuration
API_KEY: Final[str] = "c2fe5ebd74734989a33163320252302"
# BASE_URL: Final[str] = "http://api.weatherapi.com/v1/forecast.json?key=c2fe5ebd74734989a33163320252302&q=08854&days=1&aqi=no&alerts=no"
BASE_URL: Final[str] = "http://api.weatherapi.com/v1/forecast.json"
CITY: Final[str]= "New York"
COUNTRY: Final[str] = "US"

pull_request = False

# Function to fetch weather data
def fetch_weather(city: str, country: str, pull_request: bool = False):
    if pull_request:
        # params = {"q": f"{city},{country}", "appid": API_KEY, "units": "metric"}
        params = {"key":API_KEY, "q": "08854", "days": "1", "aqi": "no", "alerts": "no"}
        response = requests.get(BASE_URL, params=params)
        return response.json()
    else:
        with open(json_file_path, "r") as data:
            return load(data)

def masssage_json_data() -> None:
    """
    Add UUID to JSON Data
    """
    with open(json_file_path, mode='r') as json_obj:
        json_data = load(json_obj)

        json_data["location"]["process_uuid"] = process_uuid
        json_data["current"]["process_uuid"] = process_uuid
        json_data["forecast"]["forecastday"][0]["process_uuid"] = process_uuid
        json_data["forecast"]["forecastday"][0]["day"]["process_uuid"] = process_uuid

    # print(dumps(json_data, indent=2))

    with open(json_file_path, 'w') as json_obj:
        json_obj.write(dumps(json_data, indent=2))


def create_stage_files() -> None:
    """
    Create_stage_files
    """

    with open(json_file_path, 'r') as json_obj:        
        json_data = load(json_obj)

    flatten_json(json_data, 
                hourly_file_path, 
                record_path=['forecast', 'forecastday', 'hour'], 
                meta=[['forecast', 'forecastday', 'date'], ['forecast', 'forecastday', 'date_epoch'], ['forecast', 'forecastday', 'process_uuid']]
                )
    
    # flatten_json(json_data["forecast"]["forecastday"][0]["hour"],hourly_file_path, meta=['forecast', 'forecastday', 'process_uuid'], )

    flatten_json(json_data['forecast']['forecastday'][0]["day"], daily_file_path)

    flatten_json(json_data['location'], location_file_path)

    flatten_json(json_data['current'], currently_file_path)


def stage_load(base_file_path: Optional[str] = base_file_path) -> None:
    """
    Load files into stage tables
    """

    files_only = [file for file in base_file_path.iterdir() if file.is_file()]
    for file in files_only:
        if str(file).endswith(f"{datetime.today().strftime('%Y%m%d')}.csv"):
        # if str(file).endswith("20250303.csv"):
            file_path = file
            file_name = file.name
            file_prefix = file.name.split('_')[0]

            # print(file_path, file_name, file_prefix)

            load_stage(stage_schema, file_prefix, file_path, "|", truncate=True)
    
def main():
    weather_data = fetch_weather(CITY, COUNTRY, pull_request)

    if pull_request:
        with open(json_file_path, "w") as writej:
            writej.write(dumps(weather_data, indent=2))

    masssage_json_data()

    create_stage_files()
 
    stage_load()

    table_load(str(prep_load_script))
    
    table_load(str(target_load_script))




if __name__ == "__main__":
    main()

