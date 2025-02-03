# Databricks notebook source
# MAGIC %run ./5.Vector_DB-Gold

# COMMAND ----------

!pip install langchain[all]
!pip install Langchain[FAISS]
!pip install faiss-cpu
!pip install langchain-community
!pip install -U sentence-transformers
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain import FAISS
import pandas as pd

# COMMAND ----------

embedding=HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

# COMMAND ----------

faiss_db=FAISS.load_local('/FileStore/tables/vectordb/llm_imdb/',embeddings=embedding,allow_dangerous_deserialization=True)

# COMMAND ----------

docs=faiss_db.similarity_search_with_score('the godfather',5)

#docs=faiss_db.similarity_search_with_score('oppola which is the early life and career of vito corleone in 1920s new york city is portrayed',5)

# COMMAND ----------

docs

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Extracting Data from Tuple
document = docs[0][0]
score = float(docs[0][1])

#Preparing data to Save
data_to_save=[(document.metadata['id'],document.metadata['Series_Title'],document.metadata['Genre'],document.metadata['Director'],document.metadata['Overview'],document.page_content,score)]

schema=StructType
