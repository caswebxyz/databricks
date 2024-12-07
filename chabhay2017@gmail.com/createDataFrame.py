# Databricks notebook source
# DBTITLE 1,Create DataFrame using RDD
data=[(1,'Sri'),(2,'Abc')]
columns=['Id','Name']
rdd=spark.sparkContext.parallelize(data)
rdd.collect()


# COMMAND ----------

df=rdd.toDF()
df.show()

# COMMAND ----------

df=rdd.toDF(columns)
df.show()

# COMMAND ----------

df=spark.createDataFrame(rdd).toDF(*columns)
df.show()

# COMMAND ----------

df=spark.createDataFrame(data)
df.show()

# COMMAND ----------

df=spark.createDataFrame(data, columns)
df.show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType,IntegerType,FloatType
schema=StructType([StructField('Id',IntegerType(),True),StructField('Name',StringType(),True)])
data=[(1,'ABC'),(2,'XYZ')]
df=spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.show(truncate=True)
