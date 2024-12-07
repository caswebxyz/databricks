# Databricks notebook source
from pyspark.sql import SparkSession

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database rdrepv;
# MAGIC

# COMMAND ----------

empDF.write.format("csv").mode("overwrite").option("sep","|").saveAsTable("rdrepv.emp1")

# COMMAND ----------

empDF.write.format("csv").option("sep","|").save("/FileStore/tables/emp_folder")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_folder")

# COMMAND ----------

empDF.write.format("csv").partitionBy("emp_dept_id").option("sep","|").save("/FileStore/tables/emp_folder_test")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_folder_test")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/FileStore/tables/emp_folder_test/emp_dept_id=50/`

# COMMAND ----------

empDF.write.format("parquet").partitionBy("emp_dept_id").mode("overwrite").save("/FileStore/tables/emp_folder_test")

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/emp_folder_test")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/FileStore/tables/emp_folder_test/emp_dept_id=10/`

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended  rdrepv.emp1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rdrepv.emp1;

# COMMAND ----------

# MAGIC %sql
# MAGIC update rdrepv.emp1 set emp_dept_id=100 where emp_id=1

# COMMAND ----------

# MAGIC %sql
# MAGIC create table rdrepv.emp_delta (emp_id int,
# MAGIC name string,
# MAGIC superior_emp_id int,
# MAGIC year_joined string,
# MAGIC emp_dept_id string,
# MAGIC gender string,
# MAGIC salary int
# MAGIC )
# MAGIC using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended rdrepv.emp_delta;

# COMMAND ----------

empDF.createOrReplaceGlobalTempView("emp1_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.emp1_v;
# MAGIC insert into rdrepv.emp_delta select * from global_temp.emp1_v;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rdrepv.emp_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC update rdrepv.emp_delta set emp_dept_id=100 where emp_id=1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rdrepv.emp_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table rdrepv.emp_delta1 (emp_id int,
# MAGIC name string,
# MAGIC superior_emp_id int,
# MAGIC year_joined string,
# MAGIC emp_dept_id string,
# MAGIC gender string,
# MAGIC salary int
# MAGIC )
# MAGIC using delta
# MAGIC location '/FileStore/tables/delta/emp_delta1'

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended rdrepv.emp_delta1
