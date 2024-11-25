# Databricks notebook source
# DBTITLE 1,Run Setup
# MAGIC %run "./Setup"

# COMMAND ----------

# DBTITLE 1,Call API
# Define NASA url:
url = f'https://api.nasa.gov/insight_weather/?api_key={api_key}&feedtype=json&ver=1.0'

# Request:
r = requests.get(url)

# Load data as JSON:
data = r.json()

# COMMAND ----------

# DBTITLE 1,Establish Days
# Generate list of days in returned data:
sol_days = [key for key in data.keys() if key.isdigit()]

# COMMAND ----------

# DBTITLE 1,Data Lake
# Iterate through the returned days:
for day in sol_days:
  # Create JSON dump
  j = json.dumps(data[day])
  # Write to data lake:
  try:
    dbutils.fs.put(storageRAW + f'/{day}/{day}.json', j, overwrite=True)
  except Exception as e:
    print(e)
  



# COMMAND ----------

# DBTITLE 1,Task Values
# Hold ingested values in Dictionary
task_values = [x for x in sol_days]

# Set task output values
dbutils.jobs.taskValues.set(key = "Ingested_Values", value = task_values)

