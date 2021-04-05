from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df=spark.read.load("file:///opt/bitnami/spark/datasets/output/people")
df.show()