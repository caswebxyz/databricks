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

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"inner").show()

# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"inner").select(empDF.emp_id,(empDF.name).alias("emp_name"),deptDF.dept_name,empDF.salary).show()

# COMMAND ----------

empDF.join(deptDF,empDF.emp_dept_id==deptDF.dept_id,"right").select(empDF.emp_id,(empDF.name).alias("emp_name"),deptDF.dept_name,empDF.salary).show()

# COMMAND ----------

#inner, cross, outer,full, full_outer, left, left_outer, right, right_outer,left_semi, and left_anti.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.df1_view;
# MAGIC
# MAGIC ####--global_temp.df1_view; was created at Views Notebook, since it is global and both the notebook's are using same cluster
# MAGIC #-- we are able to query the data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df2_view;
