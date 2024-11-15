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

#Renaming circuits columns
circuits_df = circuits_df.withColumnRenamed("location","circuit_location")

#Renaming races column
races_df = races_df.withColumnRenamed("year","race_year") \
  .withColumnRenamed("name","race_name") \
  .withColumnRenamed("race_timestamp","race_date")

#Renaming drivers column
drivers_df = drivers_df.withColumnRenamed("name","driver_name") \
  .withColumnRenamed("number","driver_number") \
  .withColumnRenamed("nationality","driver_nationality")

#Renaming results columns
results_df = results_df.withColumnRenamed("time","race_time")

#Renaming constructors columns
constructors_df = constructors_df.withColumnRenamed("name","team")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Join (drivers, constructors, results) and (circuits, races)####

# COMMAND ----------



drivers_results_constructors = drivers_df.join(results_df, "driver_id").join(constructors_df, "constructor_id")

races_circuits = races_df.join(circuits_df, "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join the output of last two dataframes to results####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

combined_df = drivers_results_constructors.join(races_circuits, "race_id")

final_df = combined_df.select("race_year", "race_name", "race_date", \
  "circuit_location", \
  "driver_name", "driver_number", "driver_nationality", \
  "team", \
  "grid", "fastest_lap", "race_time", "points", "position") \
  .withColumn("created_date",current_timestamp())

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Write Results to file####

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/races_results")
