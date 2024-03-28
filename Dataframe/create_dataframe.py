from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('create dataframe').getOrCreate()

data = [("James", "Smith", "31-07-2001", "M", 60000),
        ("Michael", "Rose", "2-08-2003", "M", 70000),
        ("Robert", "Williams", "09-07-2001", "M", 400000),
        ("Maria", "Anne", "24-08-2002", "F", 500000),
        ("Jen", "Mary", "2-08-2012", "F", 2000)]

columns = ["first_name", "last_name", "dob", "gender", "salary"]

pysparkDF = spark.createDataFrame(data=data, schema=columns)
pysparkDF.printSchema()  # to see the columns
pysparkDF.show()  # to show the tables values
