# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the JSON file using the spark dataframe reader
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

# COMMAND ----------

# MAGIC %run "../../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/constructors.json")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted columns from the dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file
# MAGIC

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
