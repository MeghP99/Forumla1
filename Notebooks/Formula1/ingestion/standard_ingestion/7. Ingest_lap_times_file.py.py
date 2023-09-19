# Databricks notebook source
# MAGIC  %md
# MAGIC # Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/lap_times")


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")
