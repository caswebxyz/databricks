# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

schema = StructType([
    StructField("Description", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("value", StringType(), True),
])

# COMMAND ----------

cloud_file_options ={
    "cloudFiles.format":"csv"
}

# COMMAND ----------

@dlt.table
def bronze_load():
    df = spark.readStream.format("cloudFiles").options(**cloud_file_options).schema(schema).load("/Volumes/awsdbx_w1_2358208440317044/default/v1/")
    return df


# COMMAND ----------

@dlt.table(
    name = "bronze_mv_load"
)
def bronze_static_load_():
    df =  dlt.read("bronze_load")
    return df


# COMMAND ----------

@dlt.view
def dataquality_view():
    df = dlt.readStream("bronze_load")
    return df
