# Databricks notebook source
# DBTITLE 1,Run Setup
# MAGIC %run "./Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC https://dbdiagram.io/d/mars-65980813ac844320ae51a06a
# MAGIC

# COMMAND ----------

# DBTITLE 1,Determine Days to Curate
# Generate List of days ingested into BASE
days = dbutils.jobs.taskValues.get(taskKey='Ingestion', key = 'Ingested_Values', debugValue=['675', '676', '677', '678', '679', '680', '681'])

# COMMAND ----------

# DBTITLE 1,Schema & Dataframe
# Define Schema
schema = StructType([
    StructField("Average_Temp_f", DoubleType(), True),
    StructField("Number_Temp_Samples", LongType(), True),
    StructField("Minimum_Temp_f", DoubleType(), True),
    StructField("Maximum_Temp_f", DoubleType(), True),
    StructField("First_Measurement_Date", DateType(), True),
    StructField("Last_Measurement_Date", DateType(), True),
    StructField("Average_Wind_Speed_ms", DoubleType(), True),
    StructField("Number_Wind_Samples", LongType(), True),
    StructField("Minimum_Wind_Speed_ms", DoubleType(), True),
    StructField("Maximum_Wind_Speed_ms", DoubleType(), True),
    StructField("Wind_Direction_degrees", DoubleType(), True)
])

# Create empty dataframe:
df = spark.createDataFrame([], schema=schema)

# COMMAND ----------

# DBTITLE 1,Load Dataframes
# Iterate through days
for day in days:
    # Create dataframes dynamically based on day:
    temp_df = spark.read.parquet(storageBASE + f'/{day}/')
    # Union to df
    df = df.union(temp_df)
    
# Cache dataframe:
df.cache()


# COMMAND ----------

# DBTITLE 1,Create Temp Views
# Create temp view:
df.createOrReplaceTempView('mars_tv')
# Unpersist dataframe:
df.unpersist()

# COMMAND ----------

# DBTITLE 1,DIM Date
query = """
SELECT
  ROW_NUMBER() OVER (ORDER BY Last_Measurement_Date) AS UniqueID,
  DATE_FORMAT(Last_Measurement_Date, 'yyyy') AS Year, 
  DATE_FORMAT(Last_Measurement_Date, 'MM') AS Month,
  DATE_FORMAT(Last_Measurement_Date, 'dd') AS Day  
FROM mars_tv
"""
# Create dataframe:
dim_date = spark.sql(query)
# Create temporary view:
dim_date.createOrReplaceTempView('dimDate')

# COMMAND ----------

query =  """
SELECT 
  ROUND((5.0 / 9) * (mars_tv.Average_temp_f - 32), 2) AS Average_Temp_Celsius
, mars_tv.Number_Temp_Samples AS Number_Temp_Samples
, ROUND((5.0 / 9) * (mars_tv.Minimum_Temp_f - 32), 2) AS Minimum_Temp_Celsius
, ROUND((5.0 / 9) * (mars_tv.Maximum_Temp_f - 32), 2) AS Maximum_Temp_Celsius
, dimDate.UniqueId AS DateId
, ROUND(mars_tv.Average_Wind_Speed_ms, 2) AS Average_Wind_Speed_ms
, ROUND(mars_tv.Minimum_Wind_Speed_ms, 2) AS Minimum_Wind_Speed_ms
, ROUND(mars_tv.Maximum_Wind_Speed_ms, 2) AS Maximum_Wind_Speed_ms
, ROUND(mars_tv.Wind_Direction_degrees, 2) AS Wind_Direction_degrees
FROM mars_tv
JOIN dimDate 
ON
    YEAR(mars_tv.Last_Measurement_Date) = dimDate.Year
    AND MONTH(mars_tv.Last_Measurement_Date) = dimDate.Month
    AND DAY(mars_tv.Last_Measurement_Date) = dimDate.Day

"""
fact = spark.sql(query)
fact.createOrReplaceTempView('fact')

# COMMAND ----------

# DBTITLE 1,Merge Into mars_fact
# MAGIC %sql
# MAGIC MERGE INTO mars_fact
# MAGIC USING fact
# MAGIC ON
# MAGIC     fact.DateId = mars_fact.Date_Id 
# MAGIC AND fact.Average_Temp_Celsius = mars_fact.Average_Temp_Celsius
# MAGIC AND fact.Number_Temp_Samples = mars_fact.Number_Temp_Samples
# MAGIC AND fact.Minimum_Temp_Celsius = mars_fact.Minimum_Temp_Celsius
# MAGIC AND fact.Maximum_Temp_Celsius = mars_fact.Maximum_Temp_Celsius
# MAGIC AND fact.Average_Wind_Speed_ms = mars_fact.Average_Wind_Speed_ms
# MAGIC AND fact.Minimum_Wind_Speed_ms = mars_fact.Minimum_Wind_Speed_ms
# MAGIC AND fact.Maximum_Wind_Speed_ms = mars_fact.Maximum_Wind_Speed_ms
# MAGIC AND fact.Wind_Direction_degrees = mars_fact.Wind_Direction_degrees
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET 
# MAGIC     mars_fact.Average_Temp_Celsius = fact.Average_Temp_Celsius,
# MAGIC     mars_fact.Number_Temp_Samples = fact.Number_Temp_Samples,
# MAGIC     mars_fact.Minimum_Temp_Celsius = fact.Minimum_Temp_Celsius,
# MAGIC     mars_fact.Maximum_Temp_Celsius = fact.Maximum_Temp_Celsius,
# MAGIC     mars_fact.Average_Wind_Speed_ms = fact.Average_Wind_Speed_ms,
# MAGIC     mars_fact.Minimum_Wind_Speed_ms = fact.Minimum_Wind_Speed_ms,
# MAGIC     mars_fact.Maximum_Wind_Speed_ms = fact.Maximum_Wind_Speed_ms,
# MAGIC     mars_fact.Wind_Direction_degrees = fact.Wind_Direction_degrees,
# MAGIC     mars_fact.Date_Id = fact.DateId
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (Date_Id, Average_Temp_Celsius, Number_Temp_Samples, Minimum_Temp_Celsius, Maximum_Temp_Celsius, Average_Wind_Speed_ms, Minimum_Wind_Speed_ms, Maximum_Wind_Speed_ms, Wind_Direction_degrees)
# MAGIC   VALUES (fact.DateId, fact.Average_Temp_Celsius, fact.Number_Temp_Samples, fact.Minimum_Temp_Celsius, fact.Maximum_Temp_Celsius, fact.Average_Wind_Speed_ms, fact.Minimum_Wind_Speed_ms, fact.Maximum_Wind_Speed_ms, fact.Wind_Direction_degrees);

# COMMAND ----------

# DBTITLE 1,Merge Into dim_date
# MAGIC %sql
# MAGIC MERGE INTO date_dim
# MAGIC USING dimDate
# MAGIC ON
# MAGIC     dimDate.UniqueID = date_dim.UniqueID
# MAGIC     AND dimDate.Year = date_dim.Year
# MAGIC     AND dimDate.Month = date_dim.Month
# MAGIC     AND dimDate.Day = date_dim.Day
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET 
# MAGIC     date_dim.UniqueID = dimDate.UniqueID,
# MAGIC     date_dim.Year = dimDate.Year,
# MAGIC     date_dim.Month = dimDate.Month,
# MAGIC     date_dim.Day = dimDate.Day
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (UniqueId, Year, Month, Day)
# MAGIC     VALUES (dimDate.UniqueID, dimDate.Year, dimDate.Month, dimDate.Day);
