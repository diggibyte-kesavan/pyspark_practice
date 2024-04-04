from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create rdd').getOrCreate()

# create rdd

rdd = spark.sparkContext.range(1, 5)
print(rdd.collect())

# Create RDD using sparkContext.parallelize

data = [1, 2, 3, 5, 67, 89, 90, 45]
rdd1 = spark.sparkContext.parallelize(data)
print(rdd1.collect())

# get & give the number of partition count using getNumPartition function

rdd_num_parition = spark.sparkContext.parallelize([1, 2, 43, 5, 6], 5)
repart_rdd = rdd_num_parition.repartition(4)  # repartition the RDD
print(repart_rdd.getNumPartitions())
print(rdd_num_parition.getNumPartitions())

# create a rdd using sparkContext.textfile

rdd2 = spark.sparkContext.textFile(r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\text01.txt')
print(rdd2.collect())

# create a rdd using sparkContext.wholeTextFiles

rdd3 = spark.sparkContext.wholeTextFiles(r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\text01.txt')
print(rdd3.collect())
print(rdd3.getNumPartitions())

# transformations

read_txt_rdd = spark.sparkContext.textFile(r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\text02.txt')
print(read_txt_rdd.collect())

# flatmap transformation
rdd_flat = read_txt_rdd.flatMap(lambda x: x.split(" "))
print(rdd_flat.collect())

# map transformation
rdd_map = read_txt_rdd.map(lambda x: (x, 1))
print(rdd_map.collect())

# reducebykey( merges the values for each key with the function specified,and count the same letter)
rdd_reducebykey = rdd_map.reduceByKey(lambda a, b: a + b)
print(rdd_reducebykey.collect())

# sortbykey transformation (sortByKey() transformation is used to sort RDD elements on key. In our example, first,
# we convert RDD[(String,Int]) to RDD[(Int, String]) using map transformation and apply sortByKey)
rdd_sortbykey = rdd_reducebykey.map(lambda x: (x[1], x[0])).sortByKey()
print(rdd_sortbykey.collect())

# filter transformation
rdd_filter = rdd_sortbykey.filter(lambda  x: 'an' in x[1])
print(rdd_filter.collect())