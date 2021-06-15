# Databricks notebook source
# MAGIC %run ./Create_User_DB

# COMMAND ----------

# Get the email address entered by the user on the calling notebook
db_name = spark.conf.get("com.databricks.training.spark.dbName")

# Get user name

#username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
#username_replaced = username.replace(".", "_").replace("@","_")
username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
#username = dbutils.widgets.get("user_name")
base_table_path = f"{username}/deltademoasset/"
local_data_path = f"{username}/deltademoasset/"

# Construct the unique database name
database_name = db_name
print(f"Database Name: {database_name}")

# DBFS Path is
print(f"DBFS Path is: {base_table_path}")

#Local Data path is
print(f"Local Data Path is: {local_data_path}")


# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

#donwload the file to local file path and move it to DBFS

import subprocess


# Delete local directories that may be present from a previous run

process = subprocess.Popen(['rm', '-f', '-r', local_data_path],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')




# COMMAND ----------

# Create local directories used in the workshop

process = subprocess.Popen(['mkdir', '-p', local_data_path],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.sys.process._
# MAGIC 
# MAGIC val username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
# MAGIC 
# MAGIC val base_table_path = "dbfs:/FileStore/" + username + "/deltademoasset/"
# MAGIC 
# MAGIC val url = "https://drive.google.com/file/d/1MlvaFVK8zdKMyMCVuBvjmYfjQ5zFy3te/view?usp=sharing"
# MAGIC 
# MAGIC val localpath = "/tmp/" + username + "_historical_sensor_data.csv"
# MAGIC 
# MAGIC dbutils.fs.mkdirs(base_table_path)
# MAGIC 
# MAGIC "wget -O " + localpath + " " + url !!
# MAGIC 
# MAGIC dbutils.fs.cp("file:" + localpath, base_table_path)

# COMMAND ----------

# Only for DropBox
#process = subprocess.Popen(['wget', '-P', local_data_path, 'https://www.dropbox.com/s/50q27gaifx10wqn/historical_sensor_data.csv'],
#                     stdout=subprocess.PIPE, 
#                     stderr=subprocess.PIPE)
#stdout, stderr = process.communicate()
#
#stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

# Copy the downloaded data to DBFS - Only for Dropbox

#dbutils.fs.rm(f"dbfs:/FileStore/{base_table_path}historical_sensor_data.csv")

#dbutils.fs.cp(f"file:/databricks/driver/{local_data_path}historical_sensor_data.csv", f"dbfs:/FileStore/{base_table_path}historical_sensor_data.csv")

# COMMAND ----------

# Only for Dropbox

#process = subprocess.Popen(['wget', '-P', local_data_path, 'https://www.dropbox.com/s/30m8ay9zp4z8uo2/backfill_sensor_data_final.csv'],
#                     stdout=subprocess.PIPE, 
#                     stderr=subprocess.PIPE)
#stdout, stderr = process.communicate()
#
#stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

# Copy the downloaded data to DBFS - Only for Dropbox

#dbutils.fs.rm(f"dbfs:/FileStore/{base_table_path}backfill_sensor_data.csv")
#
#dbutils.fs.cp(f"file:/databricks/driver/{local_data_path}backfill_sensor_data_final.csv", f"dbfs:/FileStore/{base_table_path}backfill_sensor_data.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.sys.process._
# MAGIC 
# MAGIC val username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
# MAGIC 
# MAGIC val base_table_path = "dbfs:/FileStore/" + username + "/deltademoasset/"
# MAGIC 
# MAGIC val url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQ5D4FqrPCBh3Qjr48At9rMKfXewSmaB0EuoyJkT-GfF9SWXFSPBn73OcJWtM14q-fGwhxBzjdxLWEZ/pub?gid=2020573648&single=true&output=csv"
# MAGIC 
# MAGIC val localpath = "/tmp/" + username + "_backfill_sensor_data.csv"
# MAGIC 
# MAGIC dbutils.fs.mkdirs(base_table_path)
# MAGIC 
# MAGIC "wget -O " + localpath + " " + url !!
# MAGIC 
# MAGIC dbutils.fs.cp("file:" + localpath, base_table_path)

# COMMAND ----------

# Download Initial CSV file used in the workshop - Only for Dropbox
#process = subprocess.Popen(['wget', '-P', local_data_path, 'https://www.dropbox.com/s/miq89d5oaqz27ct/sensor_readings_current_labeled.csv'],
#                     stdout=subprocess.PIPE, 
#                     stderr=subprocess.PIPE)
#stdout, stderr = process.communicate()
#
#
#
#stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

# Copy the downloaded data to DBFS - - Only for Dropbox

#dbutils.fs.rm(f"dbfs:/FileStore/{base_table_path}sensor_readings_current_labeled.csv")
#
#dbutils.fs.cp(f"file:/databricks/driver/{local_data_path}sensor_readings_current_labeled.csv", f"dbfs:/FileStore/{base_table_path}sensor_readings_current_labeled.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.sys.process._
# MAGIC 
# MAGIC val username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
# MAGIC 
# MAGIC val base_table_path = "dbfs:/FileStore/" + username + "/deltademoasset/"
# MAGIC 
# MAGIC val url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRQ5uTMt0D05SvbOMuaY8TooD9ZEyaa5FBKj-fPljHCsMHG5LeC1HoPiTvy2Sqyk1KOrrlEABwruvUN/pub?output=csv"
# MAGIC 
# MAGIC val localpath = "/tmp/" + username + "_sensor_readings_current_labeled.csv"
# MAGIC 
# MAGIC dbutils.fs.mkdirs(base_table_path)
# MAGIC 
# MAGIC "wget -O " + localpath + " " + url !!
# MAGIC 
# MAGIC dbutils.fs.cp("file:" + localpath, base_table_path)

# COMMAND ----------

#Download the Plant dimension data - For DropBox

#process = subprocess.Popen(['wget', '-P', local_data_path, 'https://www.dropbox.com/s/bt78cb0vpq0x6u4/plant_data.csv'],
#                     stdout=subprocess.PIPE, 
#                     stderr=subprocess.PIPE)
#stdout, stderr = process.communicate()
#
#stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

# Copy the downloaded data to DBFS - For DropBox

#dbutils.fs.rm(f"dbfs:/FileStore/{base_table_path}plant_data.csv")
#
#dbutils.fs.cp(f"file:/databricks/driver/{local_data_path}/plant_data.csv", f"dbfs:/FileStore/{base_table_path}plant_data.csv")

# COMMAND ----------

# MAGIC %scala
# MAGIC import scala.sys.process._
# MAGIC 
# MAGIC val username = spark.conf.get("com.databricks.training.spark.userName").replace('.', '_')
# MAGIC 
# MAGIC val base_table_path = "dbfs:/FileStore/" + username + "/deltademoasset/"
# MAGIC 
# MAGIC val url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vR_nvpks51QF8D4lJe2rEc4kWr8QyEP-UjmCHUAH_GgHkEVFXgmWVLTSctZ-hYlf4curo3ZVjdpE_5p/pub?gid=1242114572&single=true&output=csv"
# MAGIC 
# MAGIC val localpath = "/tmp/" + username + "_plant_data.csv"
# MAGIC 
# MAGIC dbutils.fs.mkdirs(base_table_path)
# MAGIC 
# MAGIC "wget -O " + localpath + " " + url !!
# MAGIC 
# MAGIC dbutils.fs.cp("file:" + localpath, base_table_path)

# COMMAND ----------

# MAGIC %fs ls /FileStore/deepak_sekar/deltademoasset/

# COMMAND ----------

dataPath1 = f"dbfs:/FileStore/{base_table_path}/deepak_sekar_plant_data.csv"

df1 = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath1)

display(df1)

# COMMAND ----------

df1.createOrReplaceTempView("plant_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS dim_plant;
# MAGIC 
# MAGIC CREATE TABLE dim_plant 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT * FROM plant_vw
# MAGIC )

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database

response = local_data_path + " " + base_table_path + " " + database_name

dbutils.notebook.exit(response)
