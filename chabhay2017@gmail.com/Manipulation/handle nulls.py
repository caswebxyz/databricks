# Databricks notebook source
from pyspark.sql.types import IntegerType, StringType
data = [(1,'Abc',20),(2,None,30),(None,'Abc',None)]
schema = "ID int, Name string, Age int"

df = spark.createDataFrame(data=data,schema=schema)

df.dropDuplicates().show()

# COMMAND ----------

df.na.fill(0).show()
# only Integer DataTypes filled

# COMMAND ----------

df.na.fill("0").show()
# only String DataTypes filled

# COMMAND ----------

df.na.fill(0,["ID"]).show()
# ID Sepcific

# COMMAND ----------

df.na.fill({"ID":0,"Name":999}).show()
# multiple columns

# COMMAND ----------

df.fillna({"ID":0,"Name":999}).show()
# multiple columns passing only Single Dictionary instead of 
