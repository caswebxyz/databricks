# Databricks notebook source

data=[(1,'ABC',['IT','HR']),(2,'xyz',['A','B']),(3,'None',['C','D'])]
schema="id int,name string,dept array<string>"
df=spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import explode
df_final=df.select("ID","Name",explode("Dept"))
df_final.show()
