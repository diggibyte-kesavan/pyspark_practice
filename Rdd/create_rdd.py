from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create rdd').getOrCreate()

# create rdd

rdd = spark.sparkContext.range(1, 5)
print(rdd.collect())

# Create RDD using sparkContext.parallelize

data = [1, 2, 3, 5, 67, 89, 90, 45]
rdd1 = spark.sparkContext.parallelize(data)
print(rdd1.collect())

# create a rdd using sparkContext.textfile

rdd2 = spark.sparkContext.textFile(r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\text01.txt')
print(rdd2.collect())

# create a rdd using sparkContext.wholeTextFiles

rdd3 = spark.sparkContext.wholeTextFiles(r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\text01.txt')
print(rdd3.collect())