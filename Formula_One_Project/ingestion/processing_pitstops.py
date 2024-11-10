# Databricks notebook source
# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Read in the Data####

# COMMAND ----------

data_path = raw_folder_path
processed_data_path = processed_folder_path

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType

pitstops_schema = StructType([
  StructField("driverId",IntegerType(),True),
  StructField("duration",StringType(),True),
  StructField("lap",IntegerType(),True),
  StructField("milliseconds",IntegerType(),True),
  StructField("raceId",IntegerType(),True),
  StructField("stop",IntegerType(),True),
  StructField("time",StringType(),True)
  ])
pitstops_df = spark.read \
  .option("multiLine", True) \
  .schema(pitstops_schema) \
  .json(f"{data_path}/pit_stops.json")

#display(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename and Filter the Data####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

pitstops_date = pitstops_df.withColumn("date", current_timestamp())

pitstops_final = pitstops_date.withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("raceId", "race_id")

#display(pitstops_final)#

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Write Data to Catalog####

# COMMAND ----------

pitstops_final.write.mode("overwrite").parquet(f"{processed_data_path}/pit_stops")

#display(spark.read.parquet(f"{data_path}/pit_stops"))
