from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)

def parseLine(line):
    ## split & parse logic temp F -> C
        #map value
    ##
    return (stationID, entryType, temperature)

### station_id, , type, fahrenheit, ...
lines = sc.textFile("file:///opt/bitnami/spark/datasets/1800.csv")
parsedLines = lines.map(parseLine)

# Filter only TMIN
# Map for tuple / Mapvalues for (a,b)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
for result in stationTemps.collect():
    print(result[0] + "\t{:.2f}F".format(result[1]))

#reduceByKey - Find min()

