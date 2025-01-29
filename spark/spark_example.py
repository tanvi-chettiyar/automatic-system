from pyspark.sql import SparkSession
import logging

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .appName("YourAppName") \
    .getOrCreate()

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

text_file = spark.sparkContext.textFile("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/sam.txt")

word_counts = text_file.flatMap(lambda line: line.split()) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b)

logging.basicConfig(filename= 'first_spark_session.log', level= logging.INFO, filemode= 'a', 
                    format= '%(asctime)s - %(levelname)s - %(message)s')

for word, count in word_counts.collect():
    logging.info(f'{word}: {count}')

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