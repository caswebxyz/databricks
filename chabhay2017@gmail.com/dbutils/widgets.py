# Databricks notebook source
dbutils.widgets.dropdown("name","srini",["srini","ste"])

# COMMAND ----------

dbutils.widgets.get("name")

# COMMAND ----------

dbutils.widgets.text("Enter Value","chal")

# COMMAND ----------

dbutils.widgets.get("Enter Value")

# COMMAND ----------

dbutils.widgets.getAll()
