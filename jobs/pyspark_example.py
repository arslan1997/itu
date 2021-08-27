# Importing required modules
from pyspark.sql import SparkSession

# Building Spark Session (Function)
spark = SparkSession \
    .builder \
    .appName('TRANSACTIONS_BY_MONTH App') \
    .getOrCreate()

# Loading data into dataframe
transactions_by_month = spark.sql("SELECT * FROM bda_lab.transactions_by_month")

# Creating temp view
transactions_by_month.createOrReplaceTempView("transactions_by_month_tmp_tbl")

# Transforming data
df_transformed = spark.sql("""
    SELECT 
        item_type, 
        month, 
        sum(quantity) as total_products_sold,
        sum(total_price) as total_revenue 
    FROM transactions_by_month_tmp_tbl 
    GROUP BY item_type, month
    ORDER BY item_type
""")

# Display Original Loaded Data
transactions_by_month.show(20, truncate=0)

# Displaying results after transformation
df_transformed.show(30, truncate=0)

