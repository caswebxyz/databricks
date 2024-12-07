# Databricks notebook source
df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.show()
df.schema

# COMMAND ----------

from pyspark.sql.functions import col, lit, when
df.withColumn("Voting",when((col("Age")>22),lit('Eligible For Voting')).otherwise(lit('Not Eligible'))).show()

# COMMAND ----------

df.withColumn(
    "Voting",
    when(((col("Age") > 22) & (col("ID") > 1)), lit("Eligible For Voting")).otherwise(
        lit("Not Eligible")
    ),
).show()

# COMMAND ----------

df.selectExpr("*","case when age > 22 then 'eligible' else 'Not Eligible' end as Voting ").show()

# COMMAND ----------


