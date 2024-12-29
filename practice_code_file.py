# input_file = open('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt', 'r')
# output_file = open('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_untitled.txt', 'w')
# for contents in input_file.readlines():
#     output_file.write(contents)
# input_file.close()
# output_file.close()

# import pandas

# pandas_dataframe = pandas.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt', sep= ',')
# pandas_dataframe.to_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_df.txt', sep= '|', header= True, index = False, mode= 'w')

# import duckdb

# duckdb_dataframe = duckdb.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt', sep= ',')
# duckdb_dataframe.to_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_duckDb.txt', sep= '|', header= True)

# import polars

# polars_dataframe = polars.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt', separator= ',')
# polars_dataframe.write_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_polars.txt', separator= '|', include_header= True)

# import dask.dataframe as daskdf

# dask_df = daskdf.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt')
# dask_df.to_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_dask.txt', sep= '|', index= False, header= True)

# import pyarrow.csv as pycsv

# pyarrow_dataframe = pycsv.read_csv('/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student.txt')
# pycsv.write_csv(pyarrow_dataframe, '/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/student_pyarrow.txt', write_options = pycsv.WriteOptions(delimiter = '|'))


from base.list_class import ListDatatype
from base.string_class import StringDatatype
from base.dictionary_class import DictionaryDatatype
from base.tuple_class import TupleDatatype


class DataTypes(ListDatatype, StringDatatype, DictionaryDatatype, TupleDatatype):
    def __init__(self):
        pass