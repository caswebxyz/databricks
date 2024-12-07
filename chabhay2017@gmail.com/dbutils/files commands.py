# Databricks notebook source
dbutils.fs.ls("/FileStore/")

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/srini")

# COMMAND ----------

dbutils.fs.put("/dirloc/a.txt","This is Srinivas",True)

# COMMAND ----------

dbutils.fs.ls("/dirloc/")

# COMMAND ----------

dbutils.fs.head("/dirloc/a.txt")

# COMMAND ----------

dbutils.fs.cp("/dirloc/a.txt","/FileStore/srini")

# COMMAND ----------

dbutils.fs.ls("/FileStore/srini")

# COMMAND ----------

dbutils.fs.cp("/dirloc","/FileStore/srini",True)

# COMMAND ----------

dbutils.fs.rm("/dirloc",True)

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------


