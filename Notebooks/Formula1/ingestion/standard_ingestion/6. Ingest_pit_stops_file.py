# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/pit_stops.json")

pit_stops_df = spark.read \
.option("multiLine", True) \
.json(f"{raw_folder_path}/pit_stops.json")


# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 2 - Rename columns and add new columns
# MAGIC  1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")
