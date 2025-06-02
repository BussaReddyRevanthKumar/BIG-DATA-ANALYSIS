
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, desc, month

spark = SparkSession.builder.appName("Big Data Analysis with PySpark").getOrCreate()
df = spark.read.option("header", "true").option("inferSchema", "true").csv("synthetic_sales_data.csv")
df = df.withColumn("total_price", col("price") * col("quantity"))

total_transactions = df.count()
total_revenue = df.agg(_sum("total_price")).collect()[0][0]
average_transaction_value = total_revenue / total_transactions

top_categories = df.groupBy("product_category").agg(_sum("total_price").alias("category_revenue")).orderBy(desc("category_revenue")).limit(5)
monthly_revenue = df.withColumn("month", month("transaction_date")).groupBy("month").agg(_sum("total_price").alias("monthly_revenue")).orderBy("month")

print(f"Total Transactions: {total_transactions}")
print(f"Total Revenue: ${total_revenue:,.2f}")
print(f"Average Transaction Value: ${average_transaction_value:,.2f}")

print("Top 5 Product Categories by Revenue:")
top_categories.show()

print("Monthly Revenue Trend:")
monthly_revenue.show()

spark.stop()
