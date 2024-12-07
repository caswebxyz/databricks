# Databricks notebook source
dbutils.fs.head('/FileStore/tables/data/raw_data.csv',100)

# COMMAND ----------

df=spark.read.format('csv').option('header',True).load('/FileStore/tables/data/raw_data.csv')
df.show()

# COMMAND ----------

df=spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.schema

# COMMAND ----------

df.columns

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv').limit(1)
df.schema

# COMMAND ----------

df1=spark.read.format('json').load('/FileStore/tables/data/raw_data.json')
df.show()

# COMMAND ----------

from pyspark.sql.types import *
df1=spark.read.format('json').option("multiline",True).load('/FileStore/tables/data/raw_data.json')
df1.show()

# COMMAND ----------

df_xml=spark.read.format('xml').option('rootTag','root').option('rowTag','row').load('/FileStore/tables/data/raw_data.xml')
df_xml.show()

# COMMAND ----------

display(df_xml)
