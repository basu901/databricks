# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Read in the Data####

# COMMAND ----------

data_path = "/Volumes/demo_catalog/default/formula_one_files"

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

pitstops_final.write.mode("overwrite").parquet(f"{data_path}/pit_stops")

#display(spark.read.parquet(f"{data_path}/pit_stops"))
