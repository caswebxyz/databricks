# Databricks notebook source
df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.show()
df.schema

# COMMAND ----------

def f_add_constant(input_string:str):
  return input_string+"abc"

# COMMAND ----------

from pyspark.sql.functions import col
df.withColumn("new_col",f_add_constant(col("Name"))).show()

# COMMAND ----------

from pyspark.sql.functions import StringType
@udf(returnType=StringType())
def f_add_constant(input_string:str):
  return input_string+"abc"

# COMMAND ----------

from pyspark.sql.functions import col
df.withColumn("new_col",f_add_constant(col("Name"))).show()

# COMMAND ----------

from pyspark.sql.types import StringType
def f_add3(input_string:str):
  return input_string+"xyz"


# COMMAND ----------

from pyspark.sql.functions import udf
f_add1 = udf(lambda x:f_add3(x),StringType())

# COMMAND ----------

df.withColumn("new_col1",f_add1(col("Name"))).show()
