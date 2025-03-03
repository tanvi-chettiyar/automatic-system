import sys,os
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pathlib import Path
from json import load
from pandas import DataFrame as DF, json_normalize

from typing import Any, List, Dict, Optional, Union
from base.postgres_connector import PostgresConnection

postgres_params: Dict[str, str] = {
    'database': 'postgres',
    'host': 'localhost',
    'user': 'postgres',
    'password': 'myPassword',
    'port': '5432',
}


prep_sql = """
with stage_table as (
select * , MD5(CONCAT(city_name,region,country,latitude,longitude,timezone_id,local_time_epoch,local_time)) rec_hash
from stage.location 
)
insert into prep.location
select stg.*,
	case when target.latitude is null then 'I'
	     when stg.rec_hash <> target.rec_hash then 'U'
	     else 'N'
	end as cdc_flag
from stage_table stg 
left join target.location target on ( stg.latitude = target.latitude and stg.longitude = target.longitude );
"""

def check_file_exist(csv_file_path) -> None:
    """
    Common function to check if file exists 
    """

    if Path(csv_file_path).is_file():
        print("File Exists")
    else:
        raise FileNotFoundError("Csv File not found")


def load_stage(schema: str, table: str, csv_file_path: str, delimiter: Optional[str] = ',', truncate: Optional[bool] = False) -> None:  #Union[str, None]
    """
    This function is to load data into a staging table using psycopg2 copy_expert function. 
    WIP: optionally add header and quote char in the function arguments
    """

    load_sql = f" copy {schema}.{table} from STDIN with csv header delimiter '{delimiter}'; "

    with PostgresConnection(**postgres_params) as (conn, cur):

        if truncate:
            cur.execute(f"truncate table {schema}.{table};")
        
        check_file_exist(csv_file_path)

        with open(csv_file_path, mode='r') as data:
            cur.copy_expert(sql = load_sql, file = data)
            conn.commit()

            print(f"successfully loaded {table} to postgres from csv {csv_file_path}")


def flatten_json(json_data: str, csv_file_path, record_path: Union[str, List[str], None] = None, meta: Union[str, List[Any], None] = None, suppress_error: Optional[bool]= False, seperator: Optional[str] = '_', max_level: Optional[int] = 1) -> str:
    """
    This function uses pandas json normalise to flatten a json file and write into a csv file.

    :param
        json_data = input json_data
        csv_file_path = output csv file path
        record_path = all records you wish to extract from json
        meta = extra columns you wish to add to the csv
        suppress_error = suppess errors during json nomalize
        seperator = column name seperator
        max_level = no of level you need to flatten the json
    """
    
    json_df = json_normalize(json_data, record_path= record_path, meta= meta, errors= suppress_error, sep= seperator, max_level= max_level)
    json_df.to_csv(path_or_buf=csv_file_path, sep='|', header=True, mode='w', index=False)

    return os.path.basename(csv_file_path)
