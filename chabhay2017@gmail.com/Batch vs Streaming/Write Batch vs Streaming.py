# Databricks notebook source
# MAGIC %sql
# MAGIC --create database str_db;

# COMMAND ----------

df=spark.read.format('parquet').load("")
