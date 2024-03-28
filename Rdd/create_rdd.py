from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create rdd').getOrCreate()

rdd = spark.sparkContext.range(1, 5)  # create pyspark rdd
print(rdd.collect())
