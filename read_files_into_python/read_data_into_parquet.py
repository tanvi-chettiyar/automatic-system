## Reading and Writing paraquet files

input_text_file = '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student.txt'
input_parquet_file = '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student1.parquet'

def pandas_file():
    import pandas

    dataframe1 = pandas.read_csv(input_text_file, delimiter= ',')
    dataframe1.to_parquet('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty/student_pandas.parquet')

    dataframe2 = pandas.read_parquet(input_parquet_file)
    dataframe2.to_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty/student_pandas.csv', sep= '|')

    print(f"Pandas dataframe: \n load csv file and write parquet file \n load parquet file and write csv file")

def duckdb_file():
    import duckdb

    db_dataframe1 = duckdb.read_csv(input_text_file, header= True, sep= ',')
    db_dataframe1.write_parquet('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty/student_duckdb.parquet')

    db_dataframe2 = duckdb.read_parquet(input_parquet_file)
    db_dataframe2.write_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty/student_duckdb.csv', sep= '|', header= True)

    print(f"Duckdb dataframe: \n load csv file and write parquet file \n load parquet file and write csv file")


def polars_file():
    import polars

    polars_dataframe1 = polars.read_csv(input_text_file, has_header= True, separator= ',')
    polars_dataframe1.write_parquet('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty/student_polars.parquet')

    polars_dataframe2 = polars.read_parquet(input_parquet_file)
    polars_dataframe2.write_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty/student_polars.csv', include_header= True, separator= '|')

    print(f"Polars dataframe: \n load csv file and write parquet file \n load parquet file and write csv file")

def pyarrow_file():
    import pyarrow.csv as pycsv
    import pyarrow.parquet as pypq

    pyarrow_dataframe1 = pycsv.read_csv(input_text_file)
    pypq.write_table(pyarrow_dataframe1, where= '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty/student_pya.parquet')

    pyarrow_dataframe2 = pypq.read_table(input_parquet_file)
    pycsv.write_csv(pyarrow_dataframe2, '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty/student_pya.csv', write_options= pycsv.WriteOptions(delimiter= '|'))

    print(f"Pyarrow dataframe: \n load csv file and write parquet file \n load parquet file and write csv file")

def dask_file():
    import dask.dataframe as ddf

    dask_dataframe1 = ddf.read_csv(input_text_file)
    dask_dataframe1.to_parquet(path= '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty', write_index= False)

    dask_dataframe2 = ddf.read_parquet(path= input_parquet_file)
    dask_dataframe2.to_csv(filename= '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/empty')

    print(f"Dask dataframe: \n load csv file and write parquet file \n load parquet file and write csv file")

def main():
    list_of_functions = [pandas_file, duckdb_file, polars_file, pyarrow_file, dask_file]

    for function in list_of_functions:
        function()

if __name__ == '__main__':
    main()