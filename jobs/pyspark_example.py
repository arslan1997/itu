# Importing required modules
from pyspark.sql import SparkSession

# Building Spark Session (Function)
spark = SparkSession \
    .builder \
    .appName('PySpark-Local-App') \
    .getOrCreate()

# Loading data into dataframe
literacyrate = spark.sql("SELECT * FROM bda_lab.literacyrate")

# Creating temp view
literacyrate.createOrReplaceTempView("literacyrate_tmp_tbl")

# Transforming data
df_transformed = spark.sql("""
                    SELECT
                        SUM(males) as total_males,
                        SUM(females) as total_female
                    FROM literacyrate_tmp_tbl
                    GROUP BY districts, literacyratemales, literacyratefemales
                """)

# Displaying results
df_transformed.show(10, truncate=0)

# Getting count
#rec_count = df_transformed.count()
#print("Total Records: {0}").format(rec_count)
