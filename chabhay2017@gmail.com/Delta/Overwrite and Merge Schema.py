# Databricks notebook source
data=[(1,'Sagar'),(2,'Alex')]
schema = "Id int, Name string"
df=spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --create database demo_delta;
# MAGIC --drop table demo_delta.user_table2;

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('demo_delta.user_table2')
##df.write.mode('overwrite').saveAsTable('str_db.user_table2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_delta.user_table2;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended demo_delta.user_table2;

# COMMAND ----------

data1=[(1,'Sagar1',23),(2,'Alex1',34)]
schema1 = "Id int, Name string,age int"
df1=spark.createDataFrame(data=data1,schema=schema1)
display(df1)

# COMMAND ----------

df1.write.mode('overwrite').saveAsTable('demo_delta.user_table2')

# COMMAND ----------

df1.write.mode('overwrite').option("mergeSchema", "true").saveAsTable('demo_delta.user_table2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_delta.user_table2;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table demo_delta.user_table2;

# COMMAND ----------

data=[(1,'Sagar'),(2,'Alex')]
schema = "Id int, Name string"
df=spark.createDataFrame(data=data,schema=schema)
display(df)
df.write.mode('overwrite').saveAsTable('demo_delta.user_table2')

# COMMAND ----------

data1=[(1,'Sagar1',23),(2,'Alex1',45),(3,'kim',50)]
schema1 = "Id int, Name string,age int"
df1=spark.createDataFrame(data=data1,schema=schema1)
display(df1)

# COMMAND ----------

df1.write.mode('overwrite').option('mergeSchema',"true").saveAsTable("demo_delta.user_table2")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_delta.user_table2;

# COMMAND ----------

data2=[(1,'Sagar1')]
schema2 = "Id int, Name string"
df2=spark.createDataFrame(data=data2,schema=schema2)
display(df2)
df2.write.mode('overwrite').saveAsTable("demo_delta.user_table3")

# COMMAND ----------

data3=[(2,'Srinivas',41)]
schema3 = "Id int, Name string, age int"
df3=spark.createDataFrame(data=data3,schema=schema3)
display(df3)
df3.write.mode('append').option('mergeSchema',"true").saveAsTable("demo_delta.user_table3")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_delta.user_table3;
