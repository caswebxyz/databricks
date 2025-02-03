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

@dlt.table(
    name = "bronze_load_1"
)
def bronze_load():
    df = spark.readStream.format("cloudFiles").options(**cloud_file_options).schema(schema).load("/Volumes/awsdbx_w1_2358208440317044/default/v1/")
    return df


# COMMAND ----------

@dlt.table(
    name = "bronze_static_load_1"
)
def bronze_static_load_():
    df = spark.read.format("csv").options(**cloud_file_options).schema(schema).load("/Volumes/awsdbx_w1_2358208440317044/default/v1/")
    return df


# COMMAND ----------

@dlt.view
def dataquality_view():
    df = dlt.readStream("bronze_load_1")
    return df


# COMMAND ----------

checks = {}
checks["validate Description col for null values"] = "(Description IS NOT NULL)"
checks["validate Name col for null values"] = "(Name IS NOT NULL)"


# COMMAND ----------

##@dlt.table(
 ##   name = "dataquality_29"
##)
##@dlt.expect_or_drop("validate Description col for null values","Description IS NOT NULL")
@dlt.view
@dlt.expect_all_or_drop(checks)
def dataquality_2():
    df = dlt.readStream("bronze_load_1")
    return df


# COMMAND ----------

@dlt.table
def dataquality_3():
    df = dlt.readStream("dataquality_2")
    return df
