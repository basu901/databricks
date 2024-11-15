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



-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #Creating a Managed Table
-- MAGIC

-- COMMAND ----------

--Creating a Managed Table
USE demo;

CREATE TABLE IF NOT EXISTS 
