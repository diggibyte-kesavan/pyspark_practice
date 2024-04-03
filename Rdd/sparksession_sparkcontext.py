from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create spark session').getOrCreate()  # create spark session
print(SparkSession)

sc = spark.sparkContext  # create spark context
print("spark app name :" + sc.appName)
print(spark.sparkContext)  # SparkSession creates a SparkContext internally
print(spark.sparkContext.appName)

rdd = spark.sparkContext.range(5)  # create RDD
print(rdd.collect())
