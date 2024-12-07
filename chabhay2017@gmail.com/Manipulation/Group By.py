# Databricks notebook source
from pyspark.sql.types import IntegerType, StringType
data = [(1,'Abc',1000,'IT'),(2,'xyz',2000,'HR'),(2,'Abc',4000,'IT'),(3,'Sri',4000,'IT')]
schema = "ID int, Name string, sal int, Dept string"

df = spark.createDataFrame(data=data,schema=schema)

df.dropDuplicates().show()

# COMMAND ----------

df.groupBy("ID").count().show()
df.groupBy("Name").count().show()

# COMMAND ----------

df.groupBy("Name").sum("sal").show()
df.groupBy("ID","Name").count().show()

# COMMAND ----------

from pyspark.sql.functions import sum, count
df.groupBy("Name").agg((sum("sal").alias("Sal")),(count("Name")).alias("Count")).show()

# COMMAND ----------

from pyspark.sql.functions import sum, count, countDistinct
df.groupBy("Name").agg((sum("sal").alias("Sal")),(countDistinct("*"))).show()

# COMMAND ----------

df.groupBy("Name").agg((sum("sal").alias("Sal"))).orderBy("Sal").show()

# COMMAND ----------

from pyspark.sql.functions import sum, count, col
df.groupBy("Name").agg((sum("sal").alias("Sal"))).filter(col("Sal") > 4000).orderBy("Sal").show()
