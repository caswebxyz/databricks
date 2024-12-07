# Databricks notebook source
df = spark.read.option("multiline","true").json("/FileStore/tables/data/raw_data-1.json")
df.show()

# COMMAND ----------

df1 = spark.read.format('json').option("multiline","true").load("/FileStore/tables/data/raw_data-1.json")
df1.show()

# COMMAND ----------

df.schema

# COMMAND ----------

from pyspark.sql.types import *
## Retain order keeping ID, Name then other fields
schema = StructType(
    [
        StructField("ID", LongType(), True),
        StructField("Name", StringType(), True),
        StructField("Age", LongType(), True),
        StructField("DOB", StringType(), True)
        
    ]
)

# COMMAND ----------

df3 = spark.read.format('json').option("multiline","true").schema(schema).load("/FileStore/tables/data/raw_data-1.json")
df3.show()
