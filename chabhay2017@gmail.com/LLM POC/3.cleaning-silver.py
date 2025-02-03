# Databricks notebook source
# MAGIC %run ./1.database

# COMMAND ----------

# MAGIC %run ./2.Raw_Data-Bronze

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.table('llm_bronze.imdb_bronze')
display(df)
df.schema

# COMMAND ----------

df1=df.select('Series_Title','Genre','Director','Overview')
df1=df1.select([regexp_replace(lower(col(i)),r'[.,-:] /\\','').alias(i) for i in df1.columns])
display(df1)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

windowSpec=Window.partitionBy(lit(1)).orderBy(lit(1))

df_silver = df1.withColumn(
    "Content",
    concat(
        nvl(col("Series_Title"), lit(" ")),
        lit(" is "),
        nvl(col("Genre"), lit(" ")),
        lit(" directed by "),
        nvl(col("Director"), lit(" ")),
        lit(" which is "),
        nvl(col("Overview"), lit(" "))
    ),
).withColumn('id',concat(lit('id-'),row_number().over(windowSpec)))
display(df_silver)

# COMMAND ----------

df_silver.write.mode('overwrite').option('overwriteSchema',True).saveAsTable('llm_silver.imdb_silver')
