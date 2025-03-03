import sys, os
sys.path.insert(0,(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from json import load, dumps

from uuid import uuid4
from pathlib import Path
from helper.utils import *


process_uuid = str(uuid4())

base_file_path = Path(__file__).parent.absolute().joinpath("data", "weather")
stage_schema = 'stage'

json_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", "weather_20250228.json")

location_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", "location_20250228.csv")
current_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", "current_20250228.csv")
hourly_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", "hourly_20250228.csv")
daily_file_path = Path(__file__).parent.absolute().joinpath("data", "weather", "daily_20250228.csv")


def masssage_json_data():
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
    create_stage_files
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

    flatten_json(json_data['current'], current_file_path)


def stage_load(base_file_path: Optional[str] = base_file_path):

    files_only = [file for file in base_file_path.iterdir() if file.is_file()]
    for file in files_only:
        if str(file).endswith(".csv"):
            file_path = file
            file_name = file.name
            file_prefix = file.name.split('_')[0]

            # print(file_path, file_name, file_prefix)

            load_stage(stage_schema, file_prefix, file_path, "|", truncate=True)

def main():

    masssage_json_data()

    create_stage_files()
 
    stage_load()




if __name__ == "__main__":
    main()

