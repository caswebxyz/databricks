# Databricks notebook source
df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.show()
df.schema

# COMMAND ----------

df.drop(*["Name"]).show()

# COMMAND ----------

df.drop("Name").show()

# COMMAND ----------

df.drop("Name","ID").show()

# COMMAND ----------

df.drop(*["Name","ID"]).show()

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType
data = [(1,'Abc'),(2,'xyz'),(2,'Abc')]
schema = "ID int, Name string"

df = spark.createDataFrame(data=data,schema=schema)

df.dropDuplicates().show()

# COMMAND ----------

df.dropDuplicates(["Name"]).show()
