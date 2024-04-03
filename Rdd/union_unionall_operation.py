from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('union').getOrCreate()

rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
rdd1= spark.sparkContext.parallelize([3, 4, 5, 6])

union = rdd.union(rdd1)
print(union.collect())