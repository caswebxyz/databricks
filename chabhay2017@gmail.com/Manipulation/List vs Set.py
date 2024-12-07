# Databricks notebook source
from pyspark.sql.types import IntegerType, StringType
data = [(1,'Abc',1000,'IT'),(2,'xyz',2000,'HR'),(2,'Abc',4000,'IT'),(3,'Sri',4000,'IT'),(3,'Test',123,'IT')]
schema = "ID int, Name string, sal int, Dept string"

df = spark.createDataFrame(data=data,schema=schema)

df.dropDuplicates().show()

# COMMAND ----------

## List and Set are aggregated functions , so need to use with Group By
df1=df.groupBy("ID").sum('sal')
df1.show()

# COMMAND ----------

from pyspark.sql.functions import sum, col, collect_list, collect_set
df1=df.groupBy("ID").agg(sum('sal').alias("Salary"),collect_list("Dept"))
df1.show()

# COMMAND ----------

df1=df.groupBy("ID").agg(sum('sal').alias("Salary"),collect_set("Dept"))
df1.show()

# COMMAND ----------

data = [(1,'Abc',1000,'IT'),(2,'xyz',2000,'HR'),(2,'Abc',4000,'IT'),(3,'Sri',4000,'IT'),(3,'Sri',123,'IT')]
schema = "ID int, Name string, sal int, Dept string"
df = spark.createDataFrame(data=data,schema=schema)

df.dropDuplicates().show()
df1=df.groupBy(col("ID"),col("Name")).agg(sum('sal').alias("Salary"),collect_set("Dept"))
df1.show()
