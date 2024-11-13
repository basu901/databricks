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

from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, ArrayType

data_path = raw_folder_path
processed_data_path = processed_folder_path
v_data_source = dbutils.widgets.get("p_data_source")

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

#display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Combining first name and last name, into a single entity####

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, current_timestamp

drivers_name_combined = drivers_df.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
  .withColumnRenamed("driverRef", "driver_ref ") \
  .withColumnRenamed("driverId", "driver_id") \
  .withColumn("data_source", lit(v_data_source)) \
  .drop("url")

drivers_final = drivers_name_combined.withColumn("date",current_timestamp())

#display(drivers_final)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Write Data To Catalog####

# COMMAND ----------

drivers_final.write.mode("overwrite").parquet(f"{processed_data_path}/drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")
