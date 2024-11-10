# Databricks notebook source
# MAGIC %md
# MAGIC ####Read Data####

# COMMAND ----------

data_path = "/Volumes/demo_catalog/default/formula_one_files"

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

laptimes_schema = StructType([
  StructField("raceId",IntegerType(),True),
  StructField("driverId",IntegerType(),True),
  StructField("lap",IntegerType(),True),
  StructField("position",IntegerType(),True),
  StructField("time",StringType(),True),
  StructField("milliseconds",IntegerType(),True)
])

laptimes_initial = spark.read \
  .schema(laptimes_schema) \
  .csv(f"{data_path}/lap_times_*.csv")

#laptimes_initial.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename and Filter the Data####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

laptimes_date = laptimes_initial.withColumn("date", current_timestamp())

laptimes_final = laptimes_date.withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("raceId", "race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write Data to Catalog####

# COMMAND ----------

laptimes_final.write.mode("overwrite").parquet(f"{data_path}/lap_times")

#display(spark.read.parquet(f"{data_path}/lap_times"))
