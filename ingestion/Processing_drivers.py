# Databricks notebook source
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, ArrayType

data_path = "/Volumes/demo_catalog/default/formula_one_files"

drivers_schema = StructType([
  StructField("code", StringType(), True),
  StructField("dob", DateType(), True),
  StructField("driverId", IntegerType(), True),
  StructField("driverRef", StringType(), True),
  StructField("name",StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
  ]), True),
  StructField("nationality", StringType(), True),
  StructField("number", IntegerType(), True),
  StructField("url", StringType(), True)]
)

drivers_df = spark.read \
  .schema(drivers_schema) \
  .json(f"{data_path}/drivers.json")

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Combining first name and last name, into a single entity####

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

drivers_name_combined = drivers_df.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
  .withColumnRenamed("driverRef", "driver_ref ") \
  .withColumnRenamed("driverId", "driver_id") \
  .drop("url")

drivers_final = drivers_name_combined.withColumn("date",current_timestamp())

display(drivers_final)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Write Data To Catalog####

# COMMAND ----------

drivers_final.write.mode("overwrite").parquet(f"{data_path}/drivers_final")
