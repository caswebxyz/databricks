# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col,lit,expr
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

@dlt.table(
    name = "bronze_load_2"
)
def bronze_load():
    df = spark.readStream.format("cloudFiles").options(**cloud_file_options).schema(schema).load("/Volumes/awsdbx_w1_2358208440317044/default/v1/")
    return df


# COMMAND ----------

from os.path import join
checks = {}
checks["validate Description col for null values"] = "(Description IS NOT NULL)"
checks["validate Name col for null values"] = "(Name IS NOT NULL)"


dq_rules = "({0})".format(" AND ".join(checks.values()))
dq_rules

# COMMAND ----------

@dlt.table(
    name = "stag_silver_table"
)
@dlt.expect_all(checks)
def stag_silver_table():
    df = dlt.readStream("bronze_load_2").withColumn("is_valid", expr(dq_rules))
    return df


# COMMAND ----------

@dlt.table(
    name = "valid_silver_table"
)
@dlt.expect_all(checks)
def stag_silver_table():
    df = dlt.readStream("stag_silver_table").filter("is_valid = true")
    return df

# COMMAND ----------

@dlt.table(
    name = "Invalid_silver_table"
)
@dlt.expect_all(checks)
def stag_silver_table():
    df = dlt.readStream("stag_silver_table").filter("is_valid = false")
    return df
