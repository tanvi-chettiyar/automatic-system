import cProfile
import resource
import timeit
import json
from functools import wraps

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
        with open("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_copy_2.txt", "w") as open_file_2:
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
    # print(dataframe1)
    dataframe1.to_csv("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_df.txt", sep= "|", header = ["id","name","subject","grade"], mode= "w", index= False)

    print(f"{(datetime.now() - dt2).total_seconds()} panda file creation")


@time_and_memory_profiler
@profile
def duckdb_file():
    import duckdb

    dt2 = datetime.now()
    db_df = duckdb.read_csv(input_file, header= True, delimiter=',')
    # print(db_df)
    db_df.write_csv("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_duckDb.txt",sep="|", header=True)
    print(f"{(datetime.now() - dt2).total_seconds()} duckdb file creation")


@time_and_memory_profiler
@profile
def polars_file():
    import polars

    dt2 = datetime.now()
    p_df = polars.read_csv(input_file, has_header= True, separator= ",")
    p_df.write_csv("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_polars.txt", include_header= True, separator= "|")
    print(f"{(datetime.now() - dt2).total_seconds()} polars file creation")


@time_and_memory_profiler
@profile
def daskdataframe_file():
    from dask import dataframe as dask_df

    dt2 = datetime.now()
    d_df = dask_df.read_csv(input_file)
    d_df.to_csv("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_dask.txt", index= False, sep= "|")
    print(f"{(datetime.now() - dt2).total_seconds()} dask dataframe file creation")


@time_and_memory_profiler
@profile
def pyarrow_file():
    import pyarrow.csv as pycsv

    dt2 = datetime.now()
    pya_df = pycsv.read_csv(input_file)
    # write_options = pycsv.WriteOptions(delimiter="|") 
    pycsv.write_csv(pya_df, "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_pyarrow.txt", write_options = pycsv.WriteOptions(delimiter="|"))
    print(f"{(datetime.now() - dt2).total_seconds()} pyarrow file creation")

def main():
    python_file()
    pandas_file()
    duckdb_file()
    polars_file()
    daskdataframe_file()
    pyarrow_file()


if __name__ == "__main__":

    input_file = "/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/dataset.csv"
    # open_file = open(input_file, "r")
    # print(open_file.read())
    # print(open(input_file, "r").read())
    # open_file.close()

    main()