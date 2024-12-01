# Databricks notebook source
#file_path
sales_file_path = '/FileStore/tables/sales_csv.txt'
menu_file_path = '/FileStore/tables/menu_csv.txt'



# COMMAND ----------

#schema for sales_data
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DateType 
schema = StructType([StructField("product_id", IntegerType(), True), 
                     StructField("customer_id", StringType(), True), 
                     StructField("order_date", DateType(), True), 
                     StructField("location", StringType(), True), 
                     StructField("source_order", StringType(), True)])
sales_df=spark.read.format("csv").option("inferSchema","true").schema(schema).load(sales_file_path)
display(sales_df)


# COMMAND ----------

#schema for menu_data
schema = StructType([StructField("product_id", IntegerType(), True), 
                     StructField("product_name", StringType(), True), 
                     StructField("price", IntegerType(), True)])
menu_df=spark.read.format("csv").option("inferSchema","true").schema(schema).load(menu_file_path)
display(sales_df)

# COMMAND ----------

# DBTITLE 1,Total Amount spent by each customer
#Total Amount spent by each customer
from pyspark.sql import functions as F
df1 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(sales_df.customer_id).agg(F.sum(menu_df.price).alias('Total amount spent by the customer')).orderBy(sales_df.customer_id)
display(df1)

# COMMAND ----------

# DBTITLE 1,Total Amount spent by each food category
#Total Amount spent by each food category
df2 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(menu_df.product_name).agg(F.sum(menu_df.price).alias('Total amount spent')).orderBy(menu_df.product_name)
display(df2)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in each month
#Total Amount of sales in each month
from pyspark.sql.functions import year, month, quarter
sales_df = sales_df.withColumn("Order_month",month(sales_df.order_date))
df3 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(sales_df.Order_month).agg(F.sum(menu_df.price).alias('Total sales')).orderBy(sales_df.Order_month)
display(df3)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in each Year
#Total Amount of sales in each Year
sales_df = sales_df.withColumn("Order_year",year(sales_df.order_date))
df4 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(sales_df.Order_year).agg(F.sum(menu_df.price).alias('Total sales')).orderBy(sales_df.Order_year)
display(df4)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in every Quaterly
#Total Amount of sales in every Quaterly
sales_df = sales_df.withColumn("Order_quarter",quarter(sales_df.order_date))
df5 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(sales_df.Order_quarter).agg(F.sum(menu_df.price).alias('Total sales')).orderBy(sales_df.Order_quarter)
display(df5)

# COMMAND ----------

# DBTITLE 1,Total number of order by each category
#Total number of order by each category
df6 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(menu_df.product_name).agg(F.count(menu_df.product_name).alias('Total sales')).orderBy('Total sales',ascending=False)
display(df6)

# COMMAND ----------

# DBTITLE 1,Top 5 ordered items
#Top 5 ordered items
df7 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(menu_df.product_name).agg(F.count(menu_df.product_name).alias('Total sales')).orderBy('Total sales',ascending=False).limit(5)
display(df7)

# COMMAND ----------

# DBTITLE 1,Top ordered items
#Top ordered items
df8 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(menu_df.product_name).agg(F.count(menu_df.product_name).alias('Total sales')).orderBy('Total sales',ascending=False).limit(1)
display(df8)

# COMMAND ----------

# DBTITLE 1,Frequecy of customer visited
#Frequecy of customer visited
df9 = sales_df.filter(sales_df.source_order=='Restaurant').groupBy(sales_df.customer_id).agg(F.countDistinct(sales_df.order_date).alias('No. of times visited restaurant')).orderBy(sales_df.customer_id)
display(df9)

# COMMAND ----------

# DBTITLE 1,Total sales by each country
#Total sales by each country
df10 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(sales_df.location).agg(F.sum(menu_df.price).alias('Total amount spent by the country')).orderBy(sales_df.location)
display(df10)

# COMMAND ----------

# DBTITLE 1,Total sales by order source
#Total sales by order source
df11 = sales_df.join(menu_df,sales_df.product_id==menu_df.product_id,'inner').groupBy(sales_df.source_order).agg(F.sum(menu_df.price).alias('Total amount spent')).orderBy(sales_df.source_order)
display(df11)
