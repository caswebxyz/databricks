# Databricks notebook source
df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.show()
df.schema

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date
df1=df.withColumn("current_date", current_date())
df1.show()

# COMMAND ----------

df2=df.withColumn("current_date", current_timestamp())
df2.show()

# COMMAND ----------

from pyspark.sql.functions import year,month,day

df3=df.withColumn("Year",year("DOB")).withColumn("Month",month("DOB")).withColumn("Day",day("DOB"))
df3.show()

# COMMAND ----------

data=[(1,'2021-01-11','ABC'),(2,'2022-12-23','xyz')]
schema = "Id int, dob string, Name string"

df5 = spark.createDataFrame(data=data,schema=schema)
df5.printSchema()

# COMMAND ----------

## If u see above it is showing dob as String Type
from pyspark.sql.functions import to_date
df6=df5.withColumn("DOB1",to_date("DOB"))
df6.show()
df6.printSchema()

# COMMAND ----------

from pyspark.sql.functions import date_format,col
df6=df.withColumn("DOB1",date_format(col("DOB"),format='YYYY.MM.DD'))
df6.show()
df6.printSchema()
