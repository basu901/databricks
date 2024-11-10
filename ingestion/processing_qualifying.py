# Databricks notebook source
# MAGIC %md
# MAGIC ####Read Data####

# COMMAND ----------

data_path = "/Volumes/demo_catalog/default/formula_one_files"

from pyspark.sql.types import StructField, StructType, StringType, IntegerType

qualifying_schema = StructType([
  StructField("constructorId",IntegerType(),True),
  StructField("driverId",IntegerType(),True),
  StructField("number",IntegerType(),True),
  StructField("position",IntegerType(),True),
  StructField("q1",StringType(),True),
  StructField("q2",StringType(),True),
  StructField("q3",StringType(),True),
  StructField("qualifyId",IntegerType(),True),
  StructField("raceId",IntegerType(),True)
  ])

qualifying_df = spark.read \
  .option("multiLine", True) \
  .schema(qualifying_schema) \
  .json(f"{data_path}/qualifying*.json")

#qualifying_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Rename and Filter the Data####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

qualifying_date = qualifying_df.withColumn("date", current_timestamp())

qualifying_final = qualifying_date.withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("raceId", "race_id") \
  .withColumnRenamed("constructorId", "constructor_id") \
  .withColumnRenamed("qualifyId", "qualify_id") \


#display(qualifying_final)#

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Write Data to Catalog####

# COMMAND ----------

qualifying_final.write.mode("overwrite").parquet(f"{data_path}/qualifying")

#display(spark.read.parquet(f"{data_path}/qualifying"))
