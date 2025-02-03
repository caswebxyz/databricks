# Databricks notebook source
# MAGIC %run ./3.cleaning-silver

# COMMAND ----------

!pip install langchain[all]
!pip install Langchain[FAISS]
!pip install faiss-cpu
!pip install langchain-community

# COMMAND ----------

from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain import FAISS
import pandas as pd

# COMMAND ----------

pip install -U sentence-transformers


# COMMAND ----------

embedding=HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType, DataFrame, col

@pandas_udf("array<float>", PandasUDFType.SCALAR)
def process_embeddings(descriptions:DataFrame) -> pd.Series:
    doc_result=embedding.embed_documents(descriptions.tolist())
    return pd.Series(doc_result)


# COMMAND ----------

df_silver=spark.read.table("llm_silver.imdb_silver")
df_silver=df_silver.withColumn("embedding",process_embeddings(col('content')))
display(df_silver)


# COMMAND ----------

df_silver.write.mode('overwrite').option('overwriteSchema',True).saveAsTable("llm_silver.imdb_silver_embedding")
