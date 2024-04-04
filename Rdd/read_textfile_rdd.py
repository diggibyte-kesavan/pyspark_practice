from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('read text files').getOrCreate()

# read text file using sparkcontext.textfile

rdd = spark.sparkContext.textFile(r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\text01.txt')
print(rdd.collect())

# read csv(;) text file using sparkcontext.textfile

rdd1 = spark.sparkContext.textFile(r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\username.csv')
print(rdd1.collect())

# read csv(,) file2 with give the 1 column value

rdd2 = spark.sparkContext.textFile(r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\sample_csv_data2.csv')
print(rdd2.collect())

# read the csv file using multiple columns like structured 

rdd3 =spark.sparkContext.textFile(r'C:\Users\kesav\Documents\DE_Intern_Diggibyte\dataset\addresses.csv')
print(rdd3.collect())