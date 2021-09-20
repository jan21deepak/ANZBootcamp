# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

import dlt

@dlt.table(
  comment="Bronze data using Autoloader",  
  table_properties={
    "quality": "Bronze"
  }
)
def incremental_bronze():
  
  ## Replace this path with the value from the Batch notebook first cell output
  dbfs_data_path = "dbfs:/FileStore/deepak_sekar/bootcamp_data/"
    
  autoloader_ingest_path = f"{dbfs_data_path}/autoloader_ingest/"
  
  bronze_source_path = f"{dbfs_data_path}historical_sensor_data.csv"

  csvSchema = StructType([
    StructField("id", StringType(), True),
    StructField("reading_time", TimestampType(), True),
    StructField("device_type", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("device_operational_status", StringType(), True),
    StructField("reading_1", DoubleType(), True),
    StructField("reading_2", DoubleType(), True),
    StructField("reading_3", DoubleType(), True)
])
  return (
    # Since this is a streaming source, this table is incremental.
    spark.read.schema(csvSchema).csv(bronze_source_path)
  )


@dlt.table(
  comment="Plant Dimension",  
  table_properties={
    "quality": "Dim"
  }
)
def plant_dimension():
  
  ## Replace this path with the value from the Batch notebook first cell output
  dbfs_data_path = "dbfs:/FileStore/deepak_sekar/bootcamp_data/"
    
  plant_dim_path = f"{dbfs_data_path}/plant_data.csv"
  
  csvSchema1 = StructType([
    StructField("plant_id", IntegerType(), True),
    StructField("device_id", StringType(), True),
    StructField("plant_type", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True)
])

  return (
    # Since this is a streaming source, this table is incremental.
    spark.read.schema(csvSchema1).csv(plant_dim_path)
  )


@dlt.table(
  comment="Silver table incremental load with quality rules",  
  table_properties={
    "quality": "Silver",
    "autoOptimize": "true"
  }
)
@dlt.expect_all_or_drop({"reading1": "LIVE.incremental_bronze.reading_1 != 999.99", "reading2": "LIVE.incremental_bronze.reading_2 != 999.99", "reading3": "LIVE.incremental_bronze.reading_3 != 999.99"})
def incremental_silver():
  
  return (
    # Since this is a streaming source, this table is incremental.
    spark.sql("SELECT * FROM LIVE.incremental_bronze where device_operational_status != 'CORRUPTED'")
  )

@dlt.table(
  comment="Gold Incremental",  
  table_properties={
    "quality": "Gold"
  }
)
def incremental_gold():

  return (
    dlt.read_stream("incremental_silver").join(dlt.read("plant_dimension"), ["device_id"], "left")
  )
