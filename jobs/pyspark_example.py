# Importing required modules
from pyspark.sql import SparkSession

# Building Spark Session (Function)
spark = SparkSession \
    .builder \
    .appName('TRANSACTIONS_BY_MONTH App') \
    .getOrCreate()

# Loading data into dataframe
transactions_by_month = spark.sql("SELECT * FROM bda_lab.transactions_by_month")

# Display the loaded data
transactions_by_month.show(10, truncate=0)

# Creating temp view
transactions_by_month.createOrReplaceTempView("transactions_by_month_tmp_tbl")

# Transforming data
df_transformed = spark.sql("""
    SELECT 
        item_type, 
        month, 
        sum(quantity) as total_products_sold 
    FROM transactions_by_month_tmp_tbl 
    GROUP BY item_type, month
""")

# Displaying results
df_transformed.show(40, truncate=0)

