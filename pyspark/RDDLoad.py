from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD Load Python').getOrCreate()

sc = spark.sparkContext

lines = sc.textFile("/Users/arunma/IdeaProjects/SparkJobLoadShowTemplate/src/main/resources/NameEmailCountry.txt")

agesAsStrings = lines.map(lambda line: line.split("|")[2])
agesAsNumbers = agesAsStrings.map(lambda str: int(str))
totalAge = agesAsNumbers.reduce(lambda one, two: one + two)

lines.count()
lines.take(2)
lines.collect()
type(lines.collect())


#Creating a PairRDD
agesToNames = lines.map(lambda line: (line.split("|")[2], line.split("|")[0]))

agesToNames.reduceByKey(lambda v1, v2: v1 + "\n" + v2)

#Finding number of people in the same age
total = agesToNames.mapValues (lambda x: 1).reduceByKey(lambda v1, v2: v1 + v2)
total.collect()


print (totalAge)


total.takeOrdered(5, lambda x: x[1])
total.takeOrdered(5, lambda x: -x[1])
