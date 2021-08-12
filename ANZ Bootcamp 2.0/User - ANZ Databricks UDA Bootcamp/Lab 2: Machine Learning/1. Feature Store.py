# Databricks notebook source
# MAGIC %run ./_utils

# COMMAND ----------

# MAGIC %run "../Lab 1: Data Engineering/Utils/Fetch_User_Metadata"

# COMMAND ----------

# MAGIC %md #Step 0: Run setup code

# COMMAND ----------

setup_config = run_setup(USERNAME, DATABASE_NAME)
print(setup_config)

# COMMAND ----------

spark.sql("use {}".format(DATABASE_NAME))

# COMMAND ----------

display(sql(f'SHOW TABLES IN {DATABASE_NAME}'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Create some features

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://drive.google.com/uc?export=download&id=1EsfycP1425AZyh6Yu_OOl7AQY1w0Mcs_" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://drive.google.com/uc?export=download&id=1BZTBBqYwWzUrHF2QdDJdjnpzfGnIXy2F" width=1012/>

# COMMAND ----------

TABLE_NAME = 'sensor_readings_historical_bronze_sample'
VERSION = sql(f'describe history {DATABASE_NAME}.{TABLE_NAME} LIMIT 1').collect()[0].version

df = spark.sql(f"select * from {DATABASE_NAME}.{TABLE_NAME}")
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Let's create some windowed features on the readings

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Window

def calculate_window_features(df):
  aggs = df\
    .groupBy("device_id", window("reading_time", "5 minutes"))\
    .agg(
      mean("reading_1").alias("mean_5m_reading_1"),
      mean("reading_2").alias("mean_5m_reading_2"),
      mean("reading_3").alias("mean_5m_reading_3"),
    )

  features = df.select('id', 'reading_time')\
    .join(aggs, [df.device_id == aggs.device_id, df.reading_time >= aggs.window.end])\
    .withColumn("rank", row_number().over(Window.partitionBy(df.id).orderBy(desc(aggs.window.end))))\
    .filter(col("rank") == 1) \
    .select("id", "mean_5m_reading_1", "mean_5m_reading_2", "mean_5m_reading_3")
  
  return features

# COMMAND ----------

features = calculate_window_features(df)

display(features)

# COMMAND ----------

from databricks import feature_store

fs = feature_store.FeatureStoreClient()

fs.create_feature_table(
    name="{}.window_features".format(DATABASE_NAME),
    keys=["id"],
    features_df=features,
    description="5 minutes windowed reading features",
)

displayHTML("""
  <h3>Check out <a href="/#feature-store/{}.window_features">feature store</a></h3>
""".format(DATABASE_NAME))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Once registered, you can query features using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from window_features

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Create a training dataset

# COMMAND ----------

from databricks.feature_store import FeatureLookup

feature_table = "{}.window_features".format(DATABASE_NAME)

feature_lookups = [
    FeatureLookup( 
      table_name = feature_table,
      feature_name = "mean_5m_reading_1",
      lookup_key = ["id"],
    ),
    FeatureLookup( 
      table_name = feature_table,
      feature_name = "mean_5m_reading_2",
      lookup_key = ["id"],
    ),
  FeatureLookup( 
      table_name = feature_table,
      feature_name = "mean_5m_reading_3",
      lookup_key = ["id"],
    ),
]

training_set = fs.create_training_set(
  df,
  feature_lookups = feature_lookups,
  label = "device_operational_status"
)
 
training_df = training_set.load_df()

training_df.filter(training_df.mean_5m_reading_1.isNotNull())\
  .write.mode("overwrite").format("delta").saveAsTable(f"{DATABASE_NAME}.training_data")

display(training_df.orderBy("id"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3: What's next
# MAGIC Now that we have learnt how to use the feature store, we can go start [building ML models]($./2. Building ML Model)
