# Databricks notebook source
from DB_PKG import transformations_functions as F


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StructType, StructField,IntegerType,StringType

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)
df1 = spark.read.csv("/FileStore/tables/emp.csv", schema=schema)
df1.withColumn("load_time", current_timestamp()).withColumn(
    "file_name", input_file_name()
).show()

# COMMAND ----------

from pyspark.sql.functions import col

column_list = {"Id": "int", "Name": "string", "Age": "int"}

df_final = (
    df1.transform(F.f_cast_columns, column_list)
    .transform(F.f_add_loadtime)
    .transform(F.f_add_file_name)
)
df_final.show()
