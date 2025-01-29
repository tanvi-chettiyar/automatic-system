
# import os, sys
# import subprocess

# path1 = os.path.dirname(os.path.realpath(__file__)) + "/etl"

# path2 = os.path.dirname(os.path.realpath(__file__)) + "/etl/data"

# # path3 = os.path.dirname(os.path.relpath(__file__))

# print(path1)
# print(path2)
# # print(path3)


# sys.path.insert(0,path2)
# sys.path.append(path1)

# print(sys.path)

# # path = os.getenv("env")
# # print("PATH:", path)

# # the directory in insert is searched first and the directory in append is searched last
# # insert is safer to us than append


# from delete import Arun

# aaa = Arun()

# print(aaa)

# #sam.py

# # etl/delete.py
# # etl/data/delete.py


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .appName("YourAppName") \
    .getOrCreate()

logger = spark._jvm.org.apache.log4j.Logger.getLogger("org.apache.spark")
logger.info("This is an info log.")

text_file = spark.sparkContext.textFile("/Users/tanvi_rajkumar/Documents/GitRepo/automatic-system/etl/data/sam.txt")
word_counts = text_file.flatMap(lambda line: line.split()) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda a, b: a + b)
for word, count in word_counts.collect():
    print(f"print {word}: {count}")
    logger.info(f"log {word}: {count}")

spark.stop()