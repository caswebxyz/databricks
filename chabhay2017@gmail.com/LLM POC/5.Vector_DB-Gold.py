# Databricks notebook source
# MAGIC %run ./4.Embedding-LLM

# COMMAND ----------

!pip install langchain[all]
!pip install Langchain[FAISS]
!pip install faiss-cpu
!pip install langchain-community

# COMMAND ----------

!pip install -U sentence-transformers


df_silver=spark.read.table('llm_silver.imdb_silver_embedding').toPandas()



# COMMAND ----------

from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain import FAISS
import pandas as pd
df_silver.columns

# COMMAND ----------

list_metadata=[]
for _,row in df_silver.iterrows():
    docs={
        'id':row['id'],
        'Series_Title':row['Series_Title'],
        'Genre':row['Genre'],
        'Director':row['Director'],
        'Overview':row['Overview']
    }
    list_metadata.append(docs)


# COMMAND ----------

embedding=HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

# COMMAND ----------

faiss=FAISS.from_texts(df_silver['Content'].tolist(),embedding,list_metadata)

# COMMAND ----------

faiss.save_local('/FileStore/tables/vectordb/llm_imdb/')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/vectordb/llm_imdb/')

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/tables/vectordb/llm_imdb/")
