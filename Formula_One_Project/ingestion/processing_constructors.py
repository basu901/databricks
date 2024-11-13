# Databricks notebook source
dbutils.widgets.text("p_data_source","")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/common_functions"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Data####

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

data_path = raw_folder_path
processed_data_path = processed_folder_path
v_data_source = dbutils.widgets.get("p_data_source")

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

from pyspark.sql.functions import current_timestamp, lit

constructors_df_with_date = add_ingestion_date(constructors_df)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Filtering to Create the final Dataframe####

# COMMAND ----------

constructors_final_df = constructors_df_with_date.withColumnRenamed("constructorId","constructor_id") \
                                           .withColumnRenamed("constructorRef","constructor_ref") \
                                           .withColumn("data_source", lit(v_data_source)) \
                                          .drop("url")

#display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Writing Dataframe to Catalog#####

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet(f"{processed_data_path}/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
