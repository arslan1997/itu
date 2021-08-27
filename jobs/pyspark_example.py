# Importing required modules
from pyspark.sql import SparkSession

# Building Spark Session (Function)
spark = SparkSession \
    .builder \
    .appName('PySpark-Local-App') \
    .getOrCreate()

# Loading data into dataframe
trnasactions = spark.sql("SELECT * FROM literacyrate")

# Transforming data
#df_transformed = spark.sql(" ")

# Displaying results
df_transformed.show(10, truncate=0)

# Getting count
#rec_count = df_transformed.count()
#print("Total Records: {}").format(rec_count)
