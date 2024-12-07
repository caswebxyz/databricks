# Databricks notebook source
df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.show()
df.schema

# COMMAND ----------

from pyspark.sql.functions import col
df.orderBy(col("Name")).show()

# COMMAND ----------

df.orderBy(col("Name").desc()).show()

# COMMAND ----------

df.orderBy((col("Name").desc()),col("ID").desc()).show()
