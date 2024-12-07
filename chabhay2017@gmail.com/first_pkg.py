# Databricks notebook source
from pyspark.sql.functions import current_timestamp, input_file_name, col

def f_add_loadtime(df):
    return df.withColumn("load_time",current_timestamp())

# COMMAND ----------

def f_add_file_name(df):
    return df.withColumn("file_name",input_file_name())


# COMMAND ----------

def f_cast_columns(df, column_cast_dict: dict):
    return df.select([col(column).cast(datatype) for column, datatype in column_cast_dict.items()])

# COMMAND ----------

data=[(1,'Sri',40),(2,'Rag',50),(3,'ji',60)]
schema="id int,name string,age int"

df=spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------

df.write.csv("/FileStore/tables/emp.csv","overwrite")
df.schema

# COMMAND ----------

dbutils.fs.head("/FileStore/tables/emp*")

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
df1.show()
df1.withColumn("load_time", current_timestamp()).withColumn(
    "file_name", input_file_name()
).show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col

column_list = {
    "Id":"int",
    "Name":"string",
    "Age":"int"
}
df2=df1.select([col(column).cast(datatype) for column, datatype in column_list.items()])
df3 = df2.withColumn("load_date",current_timestamp()).withColumn("file_name",input_file_name())
df3.show()

