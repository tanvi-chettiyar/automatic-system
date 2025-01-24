import cProfile
import resource
import timeit
import json
from functools import wraps
import sys
import subprocess
import os
from pathlib import Path

from datetime import datetime

def perf(func):
    @wraps(func)
    def wrapper(*args):
        perf_calc = {"time": timeit.repeat(stmt= lambda: func(*args), repeat= 1, number=1), "memory": f"{resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024^2):.2f} MB"}
        print(json.dumps(perf_calc, indent= 2))
    return wrapper()


import time
from memory_profiler import profile

def time_and_memory_profiler(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()

        print(f"Function '{func.__name__}' took {end_time - start_time:.2f} seconds to execute.")

        return result
    return wrapper

@time_and_memory_profiler
@profile
def python_file():
    dt = datetime.now()
    with open(input_file, "r") as open_file_1:
        with open(output_file, "w") as open_file_2:
            for content in open_file_1.readlines():
                content = content.replace(",", "|")
                # print(content)
                open_file_2.write(content)
    print(f"{(datetime.now() - dt).total_seconds()} python file creation")


@time_and_memory_profiler
@profile
def pandas_file():

    import pandas
    # print(pandas.__version__)

    dt2 = datetime.now()
    dataframe1 = pandas.read_csv(input_file, delimiter= ",")
    #dataframe2 = pandas.read_parquet()
    # print(dataframe1)
    dataframe1.to_csv(output_file, sep= "|", header = ["id","name","subject","grade"], mode= "w", index= False)

    print(f"{(datetime.now() - dt2).total_seconds()} panda file creation")


@time_and_memory_profiler
@profile
def duckdb_file():
    import duckdb

    dt2 = datetime.now()
    db_df = duckdb.read_csv(input_file, header= True, delimiter=',')
    
    # print(db_df)
    db_df.write_csv(output_file,sep="|", header=True)

    db_df.write_parquet(file_name= "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student1.parquet")
    print(f"{(datetime.now() - dt2).total_seconds()} duckdb file creation")


@time_and_memory_profiler
@profile
def polars_file():
    import polars

    dt2 = datetime.now()
    p_df = polars.read_csv(input_file, has_header= True, separator= ",")
    p_df.write_csv(output_file, include_header= True, separator= "|")
    print(f"{(datetime.now() - dt2).total_seconds()} polars file creation")


@time_and_memory_profiler
@profile
def daskdataframe_file():
    from dask import dataframe as dask_df

    dt2 = datetime.now()
    d_df = dask_df.read_csv(input_file)
    d_df.to_csv(output_file, index= False, sep= "|")
    print(f"{(datetime.now() - dt2).total_seconds()} dask dataframe file creation")


@time_and_memory_profiler
@profile
def pyarrow_file():
    import pyarrow.csv as pycsv
    import pyarrow.parquet as pypq

    dt2 = datetime.now()
    pya_df = pycsv.read_csv(input_file)
    # write_options = pycsv.WriteOptions(delimiter="|") 
    pycsv.write_csv(pya_df, output_file, write_options = pycsv.WriteOptions(delimiter="|"))
    # pypq.write_table()
    print(f"{(datetime.now() - dt2).total_seconds()} pyarrow file creation")

def remove_files():
    if os.path.exists(output_file):
        # from time import sleep
        # sleep(10)
        from contextlib import suppress
        with suppress(PermissionError) as pe:
            os.remove(output_file)

def main():
    # # remove_files()
    # python_file()
    # # remove_files()
    # pandas_file()
    # # remove_files()
    # duckdb_file()
    # # remove_files()
    # polars_file()
    # # remove_files()
    # # daskdataframe_file()
    # # remove_files()
    # pyarrow_file()
    # remove_files()

    list_of_functions = [python_file, pandas_file, duckdb_file, polars_file, pyarrow_file]
    for function in list_of_functions:
        function()

    


if __name__ == "__main__":

    input_file = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student1.csv"
    output_file = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty/dataset_delete.csv"
    # open_file = open(input_file, "r")
    # print(open_file.read())
    # print(open(input_file, "r").read())
    # open_file.close()
    string = """pip install pyarrow dask[dataframe]"""
    library_list = string.split(sep=" ")
    print(library_list)

    print(sys.version)
    # subprocess.run(args=library_list)

    try:
        a = 1/0
    except Exception as e:
        print(f" error is {e}")
    finally:
        print("run something")

    main()