# Databricks notebook source
# DBTITLE 1,Run Setup
# MAGIC %run "./Setup"

# COMMAND ----------

# DBTITLE 1,Determine Days to Ingest
# Generate List of days ingested into RAW
days = dbutils.jobs.taskValues.get(taskKey='Ingestion', key = 'Ingested_Values', debugValue=['675', '676', '677', '678', '679', '680', '681'])


# COMMAND ----------

# DBTITLE 1,Read from Data Lake
def read(day):
    print(f'Reading {day}...')
    df = spark.read.json(storageRAW + f'/{day}/{day}.json')
    return df

# COMMAND ----------

# DBTITLE 1,AT - Atmospheric Temperature Sensor Summary Data
"""
av = Average of samples over the SOL (F for AT; m/s for HWS; Pa for PRE)
ct = Total number of recorded samples over the Sol
mn = Minimum data sample over the sol (same units as av)
mx = Maximum data sample over teh sol (same units as av)
"""
def temp(df):
    df = df.withColumn("Average_Temp_f", col("AT.av"))\
        .withColumn("Number_Temp_Samples", col("AT.ct"))\
        .withColumn("Minimum_Temp_f", col("AT.mn"))\
        .withColumn("Maximum_temp_f", col("AT.mx"))\
        .drop("AT")
    return df    

# COMMAND ----------

# DBTITLE 1,First / Last Measurements
def date(df):
    df = df.withColumn("First_Measurement_Date", to_date(to_timestamp(col("First_UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'")))\
        .withColumn("Last_Measurement_Date", to_date(to_timestamp(col("Last_UTC"), "yyyy-MM-dd'T'HH:mm:ss'Z'")))\
        .drop('First_UTC')\
        .drop('Last_UTC')
    return df

# COMMAND ----------

# DBTITLE 1,Horizontal Wind Speed
def windSpeed(df):
    df = df.withColumn("Average_Wind_Speed_m/s", col("HWS.av"))\
        .withColumn("Number_Wind_Samples", col("HWS.ct"))\
        .withColumn("Minimum_Wind_Speed_m/s", col("HWS.mn"))\
        .withColumn("Maximum_Wind_Speed_m/s", col("HWS.mx"))\
        .drop("HWS")
    return df

# COMMAND ----------

# DBTITLE 1,Wind Direction
def windDirection(df):
    df = df.withColumn("Wind_Direction_degrees", col("WD.most_common.compass_degrees"))\
        .drop("WD")
    return df

# COMMAND ----------

# DBTITLE 1,Drop Remaining Tables
def drop(df):
    df = df.drop("PRE")\
        .drop("Season")\
        .drop("Southern_season")\
        .drop("Month_ordinal")\
        .drop("Northern_season")
    return df

# COMMAND ----------

for day in days:
    # First read the data:
    df = read(day)
    # Put df into cache:
    df.cache()
    # Adjust for temperature:
    df = temp(df)
    # Adjust for date:
    df = date(df)
    # Adjust for windspeed:
    df = windSpeed(df)
    # Adjust for windDirection:
    df = windDirection(df)
    # Drop remaining fields:
    df = drop(df)
    # Write to data lake:
    try:
        # Write to data lake:
        df.write.format("parquet").mode("overwrite").save(storageBASE + f'/{day}/')
        # Unpersist cache:
        df.unpersist()
    except Exception as e:
        print(e)


