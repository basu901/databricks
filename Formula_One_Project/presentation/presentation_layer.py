# Databricks notebook source
# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/configuration"

# COMMAND ----------

processed_data = processed_folder_path

# COMMAND ----------

# MAGIC %md
# MAGIC ####Reading Data from Required Sources####

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races_final")
results_df = spark.read.parquet(f"{processed_folder_path}/results_final")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join circuits and races####

# COMMAND ----------

circuits_races_joined = circuits_df.join(races_df.withColumnRenamed("name","race_name") \
  .withColumnRenamed("race_timestamp","race_date"), "circuit_id").withColumnRenamed("location","circuit_location") 
display(circuits_races_joined)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join drivers and constructors and results####

# COMMAND ----------

drivers_results_joined = drivers_df.join(results_df.withColumnRenamed("time","race_time"), "driver_id") \
  .withColumnRenamed("name","driver_name") \
  .withColumnRenamed("number","driver_number") \
  .withColumnRenamed("nationality","driver_nationality")

constructors_driver_results = constructors_df.join(drivers_results_joined, "constructor_id").withColumnRenamed("name","team")
display(constructors_driver_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join the output of last two dataframes to results####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

combined_df = circuits_races_joined.join(constructors_driver_results, "race_id")

final_df = combined_df.select(circuits_races_joined.race_year, \
  circuits_races_joined.race_name, \
  circuits_races_joined.race_date, \
  circuits_df.name, \
  drivers_results_joined.driver_name, \
  drivers_df.number.alias("driver_number"), \
  drivers_results_joined.driver_nationality, \
  constructors_driver_results.team, \
  combined_df.grid, \
  combined_df.fastest_lap, \
  combined_df.race_time, \
  combined_df.points).withColumn("created_date",current_timestamp())

display(final_df)
