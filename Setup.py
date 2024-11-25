# Databricks notebook source
# DBTITLE 1,Description
"""
The purpose of this notebook is to ensure:
1. Necessary dependencies are installed. 
2. Access to blob storage is configured.
3. Correct folders exist in blob storage.
4. API key loaded
"""

# COMMAND ----------

# DBTITLE 1,Install Dependencies
import requests
import json
from pyspark.sql.functions import col, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, DateType

# COMMAND ----------

# DBTITLE 1,Security Variables
# Set Storage Variables
secret_scope         = "carbonScope"
storage_account_name = dbutils.secrets.get(scope=secret_scope, key="storageAccountName")
sas_token            = dbutils.secrets.get(scope=secret_scope, key="sasToken")
api_key              = dbutils.secrets.get(scope=secret_scope, key="apiKey")
storageRAW           = dbutils.secrets.get(scope=secret_scope, key="srtorageRAW")
storageBASE          = dbutils.secrets.get(scope=secret_scope, key="storageBASE")
storageCURATED       = dbutils.secrets.get(scope=secret_scope, key="storageCurated")
storageDbDiagram     = dbutils.secrets.get(scope=secret_scope, key="storageDbDiagram")

# COMMAND ----------

# DBTITLE 1,Spark Config
# Configure Spark to Connect to Storage Account:
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SAS") 
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider") 
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net", sas_token) 


# COMMAND ----------

# DBTITLE 1,Create Folders
"""
If folders do not exist create them.  If they do exist, do not create/delete.
"""

# Create RAW storage folder if it does not exist:
try:
  dbutils.fs.ls(storageRAW)
  print('RAW Storage Exists.')
except Exception:
  print('RAW Storage does not exist.  Creating folder...')
  try:
    dbutils.fs.mkdirs(storageRAW)
    print('Successfully created RAW folder.')
  except Exception as e:
    print(e)

# Create BASE storage folder if it does not exist:
try:
  dbutils.fs.ls(storageBASE)
  print('BASE Storage Exists.')
except Exception:
  print('BASE Storage does not exist.  Creating folder...')
  try:
    dbutils.fs.mkdirs(storageBASE)
    print('Successfully created BASE folder.')
  except Exception as e:
    print(e)


# Create CURATED storage folder if it does not exist:
try:
  dbutils.fs.ls(storageCURATED)
  print('CURATED Storage Exists.')
except Exception:
  print('CURATED Storage does not exist.  Creating folder...')
  try:
    dbutils.fs.mkdirs(storageCURATED)
    print('Successfully created CURATED folder.')
  except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Create Dim
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS date_dim (
# MAGIC     UniqueID INT,
# MAGIC     day INT,
# MAGIC     month INT,
# MAGIC     year INT
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Create Fact
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS mars_fact (
# MAGIC   Date_Id INT,
# MAGIC   Average_Temp_Celsius DOUBLE,
# MAGIC   Number_Temp_Samples INT,
# MAGIC   Minimum_Temp_Celsius DOUBLE,
# MAGIC   Maximum_Temp_Celsius DOUBLE,
# MAGIC   Average_Wind_Speed_ms DOUBLE,
# MAGIC   Minimum_Wind_Speed_ms DOUBLE,
# MAGIC   Maximum_Wind_Speed_ms  DOUBLE,
# MAGIC   Wind_Direction_degrees DOUBLE
# MAGIC );
