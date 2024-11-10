# Databricks notebook source
# MAGIC %md
# MAGIC ####Read Data####

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

data_path = "/Volumes/demo_catalog/default/formula_one_files"

constructors_schema = StructType([
  StructField("constructorId", IntegerType(), True),
  StructField("constructorRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("nationality", StringType(), True),
  StructField("url", StringType(), True),
])

constructors_df = spark.read \
                  .schema(constructors_schema) \
                  .json(f"{data_path}/constructors.json")


#constructors_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Setting the Ingestion Date####

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructors_df_with_date = constructors_df.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Filtering to Create the final Dataframe####

# COMMAND ----------

constructors_final_df = constructors_df_with_date.withColumnRenamed("constructorId","constructor_id") \
                                           .withColumnRenamed("constructorRef","constructor_ref") \
                                          .drop("url")

#display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Writing Dataframe to Catalog#####

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet(f"{data_path}/constructors_final")
