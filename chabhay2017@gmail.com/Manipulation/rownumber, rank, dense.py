# Databricks notebook source
data=[(1,'Sri'),(1,'Sri'),(2,'kim'),(3,'john'),(3,'John')]
simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# schema = "Id int, Name string"
#df=spark.createDataFrame(data=data,schema=schema)
#df.show()

# COMMAND ----------

from pyspark.sql.functions import row_number,rank,dense_rank,col
from pyspark.sql.window import Window

window_spec = Window.partitionBy(col("department")).orderBy(col("salary"))
df_final=df.withColumn("RN",row_number().over(window_spec))
df_final.show()


# COMMAND ----------

from pyspark.sql.functions import row_number,rank,dense_rank,col
from pyspark.sql.window import Window

window_spec = Window.partitionBy(col("department")).orderBy(col("salary"))
df_final=df.withColumn("RN",rank().over(window_spec))
df_final.show()

# COMMAND ----------

from pyspark.sql.functions import row_number,rank,dense_rank,col
from pyspark.sql.window import Window

window_spec = Window.partitionBy(col("department")).orderBy(col("salary"))
df_final=df.withColumn("RN",dense_rank().over(window_spec))
df_final.show()
