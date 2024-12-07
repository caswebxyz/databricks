# Databricks notebook source
df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.show()
df.schema

# COMMAND ----------

df.withColumnRenamed("ID","ID_New").show()

# COMMAND ----------

df.withColumnsRenamed({"ID":"ID_new","Name":"fullName"}).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("ID").alias("ID1")).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("ID").alias("ID1"),"*").show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("ID").alias("ID1"),"*").drop("ID").show()

# COMMAND ----------

## to Write in Sql format
df.selectExpr("ID as ID_Sql").show()

# COMMAND ----------

df.selectExpr("ID as ID_Sql","*").show()
