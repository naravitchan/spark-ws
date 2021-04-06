from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///opt/bitnami/spark/datasets/book_full.txt")
for word in lines.collect():
    print(word.encode('ascii', 'ignore'))
