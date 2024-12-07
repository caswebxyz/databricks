# Databricks notebook source
emp = [(1,"Smith",1,"IT"), \
    (2,"Rose",2,"HR"), \
    (3,"Williams",3,"IT"), \
    (4,"Jones",4,"HR"), \
    (5,"Brown",5,"IT"), \
      (6,"Brown",6,"HR") \
  ]

schema = "Id int, Name string, age int, dept string"

df=spark.createDataFrame(data=emp,schema=schema)
display(df)

df.write.partitionBy("dept").mode("overwrite").saveAsTable("demo_delta.table1")

# COMMAND ----------

# MAGIC %sql
# MAGIC --create database demo_delta;
# MAGIC select * from demo_delta.table1;

# COMMAND ----------

emp1 = [(1,"Smith",11,"IT"), (3,"Williams",33,"IT"),(5,"Brown",55,"IT") ]

schema1 = "Id int, Name string, age int, dept string"

df1=spark.createDataFrame(data=emp1,schema=schema1)

# COMMAND ----------

df1.write.partitionBy("dept").mode("overwrite").option("replaceWhere","dept=='IT'").saveAsTable("demo_delta.table1")
