# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

data_path = "/Volumes/demo_catalog/default/formula_one_files"

#Infer Schema is a costly operation, as the whole dataset needs to be scanned
#Perform only during development or a small dataset

"""circuit_df= spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{data_path}/circuits.csv")"""

circuits_schema = StructType([StructField("circuitId", IntegerType(), True),
                      StructField("circuitRef", StringType(), True),
                      StructField("name", StringType(), True),
                      StructField("location", StringType(), True),
                      StructField("country", StringType(), True),
                      StructField("lat", DoubleType(), True),
                      StructField("lng", DoubleType(), True),
                      StructField("alt", DoubleType(), True),
                      StructField("url", StringType(), True)])

circuit_df= spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{data_path}/circuits.csv")


# COMMAND ----------

circuit_df.printSchema()

# COMMAND ----------

#Another way to get an idea of the data types of the dataset
circuit_df.describe().show()

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

from pyspark.sql.functions import col

circuits_df_selected = circuit_df.select(col("circuitId"),
                                         col("circuitRef"),
                                         col("name"), 
                                         col("location"), 
                                         col("country"),
                                         col("lat"),
                                         col("lng"),
                                         col("alt"))
circuits_df_selected.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Renaming the Columns as Required###

# COMMAND ----------

circuits_df_col_renamed = circuits_df_selected.withColumnRenamed("circuitId", "circuit_id")\
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude")

display(circuits_df_col_renamed)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = circuits_df_col_renamed.withColumn("ingestion_date", current_timestamp())

#Need to define literals under "lit"
"""circuits_final_df = circuits_df_col_renamed.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("Environment",lit("Production"))"""

display(circuits_final_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Write Data as Parquet###

# COMMAND ----------


