# Databricks notebook source
df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.show()
df.schema

# COMMAND ----------


from pyspark.sql.functions import lit,col,when
df.filter(col('ID')>2).show()

# COMMAND ----------

from pyspark.sql.functions import lit,col
df.where(col('ID')>2).show()

# COMMAND ----------

from pyspark.sql.functions import lit,col,lower,rlike
df.filter(lower("Name").rlike(r'^[a-zA-Z]')).show()

# COMMAND ----------

df.filter(lower("Name").rlike(r'[a-zA-Z]m$')).show()

# COMMAND ----------

df.filter(lower("Name").rlike(r'^[a]')).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.filter((col("Name")).like('%S%')).show()
