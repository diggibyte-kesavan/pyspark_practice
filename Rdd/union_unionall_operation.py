from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('union').getOrCreate()

rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
rdd1= spark.sparkContext.parallelize([3, 4, 5, 6])

union = rdd.union(rdd1)
# union1 = rdd.union(rdd1).distinct() # using distinct keyword we remove the duplicate values
print(union.collect())
