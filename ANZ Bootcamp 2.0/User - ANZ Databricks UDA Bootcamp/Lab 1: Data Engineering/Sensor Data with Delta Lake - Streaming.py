# Databricks notebook source
# MAGIC %md
# MAGIC #### Setup

# COMMAND ----------

setup_responses = dbutils.notebook.run("./Utils/Setup-Streaming-GDrive", 0).split()

checkpoint_stream1_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]
streaming_table_path = f"{dbfs_data_path}tables/streaming"
output_sink_path = f"{dbfs_data_path}tables/streaming_output"

dbutils.fs.rm(streaming_table_path, recurse=True)
dbutils.fs.rm(checkpoint_stream1_path, recurse=True)
dbutils.fs.rm(output_sink_path, recurse=True)

print("DBFS path is {}".format(dbfs_data_path))
print("Database name is {}".format(database_name))
print("Streaming Table Location is {}".format(streaming_table_path))
print("Checkpoint Location is {}".format(checkpoint_stream1_path))
print("Output Sink Location is {}".format(output_sink_path))

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ETL Flow - Streaming
# MAGIC <img src="https://drive.google.com/uc?export=download&id=18EF_uAzQ0BwcB9abqylLu94mHddBWXtm" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Strcutured Streaming concept
# MAGIC 
# MAGIC Consider the input data stream as the “Input Table”. Every data item that is arriving on the stream is like a new row being appended to the Input Table.
# MAGIC 
# MAGIC <img src="https://s3.us-east-2.amazonaws.com/databricks-knowledge-repo-images/Streaming/continuous_streaming/cloudtrail-unbounded-tables.png" alt="" width="50%"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simulate a Stream using data in a table
# MAGIC 
# MAGIC A Delta Lake table can be a streaming source, sink, or both.
# MAGIC 
# MAGIC We're going to use a __Delta Lake table__, `readings_stream_source`, as our __stream source__.  It's just an ordinary Delta Lake table, but when we run "readStream" against it below, it will become a streaming source.
# MAGIC 
# MAGIC The table doesn't contain any data yet.  We'll initiate the stream now, and then later we'll generate data into the table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Setup 1 - Load input data for simulation

# COMMAND ----------

dataPath = f"{dbfs_data_path}/sensor_readings_current_labeled.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("streaming_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from streaming_vw

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup 2 - Landing Zone for Streaming Input Data - Bronze table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS readings_stream_source;

# COMMAND ----------

spark.sql("CREATE TABLE if not exists readings_stream_source (id INTEGER, reading_time TIMESTAMP, device_type STRING, device_id STRING, device_operational_status STRING, reading_1 DOUBLE, reading_2 DOUBLE, reading_3 DOUBLE) USING DELTA LOCATION '" + streaming_table_path + "'")

# COMMAND ----------

readings_stream = spark \
                   .readStream \
                   .format('delta') \
                   .table('readings_stream_source')

# Register the stream as a temporary view so we can run SQL on it
readings_stream.createOrReplaceTempView("readings_streaming")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * from readings_streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup 3 - Consuming streams in aggregate: Initiate 2 stream listeners
# MAGIC 
# MAGIC 
# MAGIC Take special note of the difference between __full aggregation__ and __windowed aggregation__.  The diagram below shows how windowed aggregation works:
# MAGIC 
# MAGIC <img src="https://s3.us-east-2.amazonaws.com/databricks-knowledge-repo-images/Streaming/continuous_streaming/mapping-of-event-time-to-overlapping-windows-of-length-10-mins-and-sliding-interval-5-mins.png" width="50%"/>
# MAGIC 
# MAGIC As the diagram illustrates, windowed aggregation uses only a subset of the data.  This subset is typically chosen based on a timestamp range.  Note that the timestamp is from the data itself, not from the current time of our code in this notebook.  In the diagram above, we are using a timestamp column to select data within 10-minute ranges.
# MAGIC 
# MAGIC In addition, we can __slide__ the window.  In the diagram above, we are sliding the 10-minute window every 5 minutes.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup 3.1 - Streaming - Full Aggregation

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   device_type,
# MAGIC   device_operational_status, 
# MAGIC   AVG(reading_1) AS average
# MAGIC FROM readings_streaming
# MAGIC GROUP BY device_type, device_operational_status
# MAGIC ORDER BY device_type ASC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup 3.2 - Streaming - Windowed Aggregation
# MAGIC 
# MAGIC This is also a streaming query that reads the readings_streaming stream.
# MAGIC 
# MAGIC It also calculates the average reading_1 for each device type, and also the COUNT for each device_type.
# MAGIC 
# MAGIC However, note that this average and count is NOT calculated for the entire stream, since "the beginning of time."
# MAGIC 
# MAGIC Instead, this query serves a "dashboard" use case, because it calculates the average reading over a 2-minute window of time.
# MAGIC 
# MAGIC Furthermore, this 2-minute window "slides" by a minute once per minute.
# MAGIC 
# MAGIC <img src="https://drive.google.com/uc?export=download&id=1ZeQ18Dis2uz-nH6vDnfm56vzusoXqwlu" width=1012/>

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   window,
# MAGIC   device_type,
# MAGIC   device_operational_status,
# MAGIC   count(device_type) count,
# MAGIC   avg(reading_1) average
# MAGIC FROM readings_streaming 
# MAGIC GROUP BY 
# MAGIC   WINDOW(reading_time, '2 minutes', '1 minute'),
# MAGIC   device_type,
# MAGIC   device_operational_status
# MAGIC ORDER BY 
# MAGIC   window DESC, 
# MAGIC   device_type ASC 
# MAGIC LIMIT 10 -- this lets us see the last five window aggregations for device_type

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Watermarking to Limit State while Handling Late Data
# MAGIC The arrival of late data can result in updates to older windows. This complicates the process of defining which old aggregates are not going to be updated and therefore can be dropped from the state store to limit the state size. In Apache Spark 2.1+, watermarking enables automatic dropping of old state data.
# MAGIC 
# MAGIC <img src="https://s3.us-east-2.amazonaws.com/databricks-knowledge-repo-images/Streaming/continuous_streaming/watermarking-in-windowed-grouped-aggregation.png" width="70%"/>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup 4 - Stream-Static Joins

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC   window,
# MAGIC   b.plant_id,
# MAGIC   b.plant_type,
# MAGIC   a.device_type,
# MAGIC   a.device_operational_status,
# MAGIC   count(a.device_type) count,
# MAGIC   avg(a.reading_1) average
# MAGIC FROM readings_streaming a INNER JOIN dim_plant b
# MAGIC ON a.device_id = b.device_id
# MAGIC GROUP BY 
# MAGIC   WINDOW(a.reading_time, '2 minutes', '1 minute'),
# MAGIC   a.device_type,
# MAGIC   a.device_operational_status,
# MAGIC   b.plant_id,
# MAGIC   b.plant_type
# MAGIC ORDER BY 
# MAGIC   window DESC, 
# MAGIC   a.device_type ASC 
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Setup 5 - Create a Streaming Sink

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create a checkpoint location

# COMMAND ----------

dbutils.fs.mkdirs(checkpoint_stream1_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Structured Streaming has three output modes:
# MAGIC  - append - this is used when we want to write detail-level records.  You cannot query an entire table of detail records, you can only see the latest data in the stream.
# MAGIC  - complete - however, if you query a stream with an aggregate query, Spark will retain and update the entire aggregate. In this mode, we will continue to output new versions of the entire aggregation result set.
# MAGIC  - update - this is similar to complete, and is also used for aggregations.  The difference is that "update" will only write the *changed* rows of an aggregate result set.

# COMMAND ----------

out_stream = spark.sql("""SELECT window, b.plant_id, b.plant_type, a.device_type, a.device_operational_status, count(a.device_type) count, avg(a.reading_1) average FROM readings_streaming a INNER JOIN dim_plant b GROUP BY WINDOW(a.reading_time, '2 minutes', '1 minute'), a.device_type, a.device_operational_status, b.plant_id, b.plant_type ORDER BY window DESC, a.device_type ASC LIMIT 10""")


out_stream.writeStream.format('delta').option('location', output_sink_path).option('checkpointLocation', checkpoint_stream1_path).outputMode('complete').table("readings_agg")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Examine the new sink
# MAGIC 
# MAGIC Let's take a look at the new Delta Lake table we created above using writeStream().
# MAGIC 
# MAGIC This would be a great data source for a separate dashboard application.  The application could simply query the table at will, and format the results.  It wouldn't have to worry at all about streaming or aggregation, since the information has already been prepared and formatted.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select window.start, window.end, plant_id, plant_type, device_type, count, average
# MAGIC from readings_agg

# COMMAND ----------

# MAGIC %md
# MAGIC Examine the checkpoint location

# COMMAND ----------

dbutils.fs.ls(checkpoint_stream1_path + ('/commits'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Begin generating data
# MAGIC 
# MAGIC Now we're ready to start pumping data into our stream.
# MAGIC 
# MAGIC The cell below generates transaction records into the Delta Lake table.  Our stream queries above will then consume this data.
# MAGIC 
# MAGIC Note: Do not run this untill you setup all the streaming queries (till cmd no: 23 )

# COMMAND ----------

# Now let's simulate an application that streams data into our landing_point table

import time

next_row = 0

while(next_row < 12000):
  
  time.sleep(1)

  next_row += 10
  
  spark.sql(f"""
    INSERT INTO readings_stream_source (
      SELECT * FROM current_readings_labeled
      WHERE id < {next_row} )
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Start the Stream Generation - Go To Cmd no 8

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stop All Streams
# MAGIC 
# MAGIC Note: Please do not execute this unless advised by the instructor 

# COMMAND ----------

# MAGIC %scala
# MAGIC /*
# MAGIC if (spark.streams.active.length > 0) {
# MAGIC   println("\nStopping All Active Queries...")
# MAGIC   for (s <- spark.streams.active) 
# MAGIC     s.stop
# MAGIC }
# MAGIC */
