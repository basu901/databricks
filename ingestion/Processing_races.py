# Databricks notebook source
# MAGIC %md
# MAGIC ####Load Data####

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType

data_path = "/Volumes/demo_catalog/default/formula_one_files"

race_schema = StructType([StructField("raceId", IntegerType(), True),
                        StructField("year", IntegerType(), True), 
                        StructField("round", IntegerType(), True), 
                        StructField("circuitId", IntegerType(), True), 
                        StructField("name", StringType(), True), 
                        StructField("date", StringType(), True),
                        StructField("time", StringType(), True),
                        StructField("url", StringType(), True)])

race_df = spark.read.option("header", "true") \
    .schema(race_schema) \
    .csv(f"{data_path}/races.csv")

display(race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Filtering For Selected Columns####

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, to_timestamp, lit, concat

races_selected = race_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

races_cols_added = races_selected.withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "),col("time")), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

races_final_df = races_cols_added.drop("date", "time")
#races_final_df.printSchema()

#display(races_final_df)



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ####Write Data To Parquet####

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{data_path}/races_final")

# COMMAND ----------

display(spark.read.parquet(f"{data_path}/races_final"))
