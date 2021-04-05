from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def mapper(line):
    # logic
    return Row()


lines = spark.sparkContext.textFile(
    "file:///opt/bitnami/spark/datasets/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
# get DF

# to use sql
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
    print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
# Have to stop
