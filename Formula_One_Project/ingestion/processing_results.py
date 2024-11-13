# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load Data####

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType

data_path = raw_folder_path
processed_data_path = processed_folder_path

results_schema = StructType([StructField("constructorId", IntegerType(), True),
                        StructField("driverId", IntegerType(), True), 
                        StructField("fastestLap", IntegerType(), True), 
                        StructField("fastestLapSpeed", StringType(), True), 
                        StructField("fastestLapTime", StringType(), True), 
                        StructField("grid", IntegerType(), True),
                        StructField("laps", IntegerType(), True),
                        StructField("milliseconds", IntegerType(), True),
                        StructField("number", IntegerType(), True),
                        StructField("points", DoubleType(), True),
                        StructField("position", IntegerType(), True),
                        StructField("positionOrder", IntegerType(), True),
                        StructField("positionText", StringType(), True),
                        StructField("raceId", IntegerType(), True),
                        StructField("rank", StringType(), True),
                        StructField("resultId", IntegerType(), True),
                        StructField("statusId", IntegerType(), True),
                        StructField("time", StringType(), True)])

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{data_path}/results.json")

#display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Filtering For Selected Columns####

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, concat

results_df_date = add_ingestion_date(results_df).drop("statusId")

results_df_final = results_df_date.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("data_source", lit(v_data_source))


#display(results_df_final)



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ####Write Data To Parquet####

# COMMAND ----------

results_df_final.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_data_path}/results_final")

# COMMAND ----------

dbutils.notebook.exit("Success")
