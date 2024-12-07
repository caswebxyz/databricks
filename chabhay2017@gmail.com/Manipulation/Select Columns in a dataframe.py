# Databricks notebook source
df = spark.read.format('csv').option('header',True).option('inferschema',True).load('/FileStore/tables/data/raw_data.csv')
df.show()
df.schema

# COMMAND ----------

df.select("ID","Name").show()

# COMMAND ----------

df.selectExpr("ID","Name").show()

# COMMAND ----------

from pyspark.sql.functions import col

df.select(col('ID'),col('Name')).show()

# COMMAND ----------

df[df.ID,df.Name].show()

# COMMAND ----------

df[df['ID'],df['Name']].show()

# COMMAND ----------

## I want to include all the columns exclude last two
df.select(df.columns[:-2]).show()

# COMMAND ----------

## List way to filter columns
df.select([i for i in df.columns]).show()

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, StructType, StructField
data = [(("Alex", "M"), "UP", "M"), (("Fname", "Lname"), "UK", "F")]

schema1 = StructType(
    [
        StructField("Name", StructType(
          [
          StructField("FirstName",StringType()),
          StructField("LastName",StringType())
          ]
        ) ),
        StructField("State", StringType(), True),
        StructField("Sex", StringType(), True)
    ]
)

df4=spark.createDataFrame(data=data,schema=schema1)
df4.show()

# COMMAND ----------

df4.printSchema()

# COMMAND ----------

df4.select(df4.Name).show()
df4.select(df4.Name.FirstName).show()

# COMMAND ----------

df4.select("name.*").show()
