# Databricks notebook source
dbutils.widgets.text("p_data_source","")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/configuration"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Data####

# COMMAND ----------

data_path = raw_folder_path
processed_data_path = processed_folder_path
v_data_source = dbutils.widgets.get("p_data_source")

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

from pyspark.sql.functions import current_timestamp, lit

laptimes_date = laptimes_initial.withColumn("date", current_timestamp())

laptimes_final = laptimes_date.withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("raceId", "race_id") \
  .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write Data to Catalog####

# COMMAND ----------

laptimes_final.write.mode("overwrite").parquet(f"{processed_data_path}/lap_times")

#display(spark.read.parquet(f"{data_path}/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")
