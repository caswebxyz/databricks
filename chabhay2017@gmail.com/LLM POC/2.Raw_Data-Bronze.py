# Databricks notebook source
df=spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/llm/imdb_top_1000.csv')
display(df)

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('llm_bronze.imdb_bronze')
