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

from pyspark.sql.functions import current_timestamp, lit

qualifying_date = qualifying_df.withColumn("date", current_timestamp())

qualifying_final = qualifying_date.withColumnRenamed("driverId", "driver_id") \
  .withColumnRenamed("raceId", "race_id") \
  .withColumnRenamed("constructorId", "constructor_id") \
  .withColumnRenamed("qualifyId", "qualify_id") \
  .withColumn("data_source", lit(v_data_source))


#display(qualifying_final)#

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Write Data to Catalog####

# COMMAND ----------

qualifying_final.write.mode("overwrite").parquet(f"{processed_data_path}/qualifying")

#display(spark.read.parquet(f"{data_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
