## SparkSession vs SparkContext

# SparkContext is the entry point for lower-level Spark functionality, primarily focused on RDD-based operations
# where RDD is Resilient Distributed Dataset (which is used primarily when dealing with unstructured data).

# SparkSession is the unified entry point for all high-level Spark functionality combining SparkContext, SQL Context
# and HiveContext. It is primarily focused on dataframes. SparkSession wraps SparkContext meaning you can access
# SparkContext from SparkSession. 

from pyspark.sql import SparkSession
from pyspark import SparkContext
import logging

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .appName("YourAppName") \
    .getOrCreate()

logger = spark._jvm.org.apache.log4j.Logger.getLogger("org.apache.spark")

# spark2 = SparkSession.builder \
#                 .appName("MySparkApp") \
#                 .master("local[*]") \
#                 .config("spark.driver.memory", "4g") \
#                 .config("spark.executor.memory", "4g") \
#                 .config("spark.executor.cores", "2") \
#                 .config("spark.driver.bindAddress", "127.0.0.1") \
#                 .config("spark.driver.host", "127.0.0.1") \
#                 .config("spark.sql.shuffle.partitions", "100") \
#                 .config("spark.default.parallelism", "100") \
#                 .config("spark.sql.adaptive.enabled", "true") \
#                 .getOrCreate()

sc1 = SparkContext("local[*]", "MyApp")

text_file1 = spark.sparkContest.textFile("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/sam.txt")

text_file2=  sc1.textFile("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/sam.txt") 



word_counts = text_file1.flatMap(lambda line: line.split()) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b)

# lambda is an anonymous function which we can pass in instantly without defining a name or any thing 
# like a full traditional function

logging.basicConfig(filename= 'first_spark_session.log', level= logging.INFO, filemode= 'a', 
                    format= '%(asctime)s - %(levelname)s - %(message)s')

# for logging into an external logger file use the following basic configirations

for word, count in word_counts.collect():
    logging.info(f'{word}: {count}')
    logger.info(f'log {word}: {count}')

# Dataframes in Spark -- similar to pandas and other dataframes in python

df = spark.read.csv("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student.csv", 
                    header=True, inferSchema=True)
df.show()
df.collect()
df.printSchema()
df.write.mode("overwrite").parquet("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/pysparkstudent")

df2 = spark.read.parquet("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/student1.parquet")
df2.show()
df2.collect()
df2.printSchema()

spark.stop()
sc1.stop()