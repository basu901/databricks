# Databricks notebook source
dbutils.notebook.run("/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/ingestion/processing_circuits",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/ingestion/processing_constructors",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/ingestion/processing_drivers",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/ingestion/processing_laptimes",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/ingestion/processing_pitstops",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/ingestion/processing_qualifying",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/ingestion/processing_races",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/ingestion/processing_results",0,{"p_data_source":"Ergast API"})
