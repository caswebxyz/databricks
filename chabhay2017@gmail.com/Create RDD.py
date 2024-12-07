# Databricks notebook source
list1 = [1,2,3,4]
rdd = spark.sparkContext.parallelize(list1)
rdd

# COMMAND ----------

rdd.count()

# COMMAND ----------

data_text='This is Srinivas from MIS'
dbutils.fs.put('/FileStore/tables/srini/data_sets/data_text.txt',data_text)

# COMMAND ----------

rdd = spark.sparkContext.textFile('/FileStore/tables/srini/data_sets/data_text.txt')
rdd

# COMMAND ----------

rdd.count()

# COMMAND ----------

rdd.collect()
for i in rdd.collect():
  print(i)

# COMMAND ----------

rdd2 = rdd.flatMap(lambda x: x.split(" "))
for i in rdd2.collect():
  print(i)
