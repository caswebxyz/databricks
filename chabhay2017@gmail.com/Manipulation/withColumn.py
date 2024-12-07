# Databricks notebook source
df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.show()
df.schema

# COMMAND ----------

from pyspark.sql.functions import lit,col
df.withColumn('flag',lit(0)).show()

# COMMAND ----------

df.withColumn("ID_New",col("ID")+1).show()

# COMMAND ----------

#If we want to add multiple columns - withColumn need to be repeated multiple Times
df.withColumn("ID_New",col("ID")+1).withColumn("Flag",lit(1)).show()

# COMMAND ----------

## Instead of above approach to add multiple columns we can use
df.withColumns({"ID_new":col("ID")+1,"Flag":lit(2)}).show()
