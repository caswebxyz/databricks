# Databricks notebook source
# MAGIC %sql
# MAGIC select * from demo_delta.table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history demo_delta.table1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from demo_delta.table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history demo_delta.table1;
# MAGIC --select * from demo_delta.table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --desc history demo_delta.table1;
# MAGIC select * from demo_delta.table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table demo_delta.table1 to version as of 4

# COMMAND ----------

# MAGIC %sql
# MAGIC --desc history demo_delta.table1;
# MAGIC select * from demo_delta.table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from demo_delta.table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history demo_delta.table1;
# MAGIC --select * from demo_delta.table1;

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table demo_delta.table1 to timestamp as of '2024-11-22T15:21:08.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC --desc history demo_delta.table1;
# MAGIC select * from demo_delta.table1;
