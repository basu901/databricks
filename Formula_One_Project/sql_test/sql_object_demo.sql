-- Databricks notebook source
--TO SHOW Current Database

SELECT current_database()

-- COMMAND ----------

--By Default, shows the tables in the current database
SHOW TABLES

-- COMMAND ----------

DESCRIBE EXTENDED orders_bronze

-- COMMAND ----------

-- Creating a DATABASE

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

--If we want to see the tables in a different database
SHOW TABLES in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Creating Tables###

-- COMMAND ----------

-- MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/configuration"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #####Managed Tables#####

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC data_path = presentation_folder_path
-- MAGIC
-- MAGIC #Creating a Managed Table
-- MAGIC race_data = spark.read.parquet(f"{data_path}/races_results")
-- MAGIC race_data.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

--You can use the database name in the namespace of the table for view, even if the current database is different

DESCRIBE EXTENDED demo.race_results_python

-- COMMAND ----------

USE demo;

SHOW TABLES

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python
ORDER BY points DESC

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT * FROM demo.race_results_python
WHERE race_year=2020

-- COMMAND ----------

SHOW TABLES in demo
