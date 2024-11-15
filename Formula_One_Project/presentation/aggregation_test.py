# Databricks notebook source
# MAGIC %run "/Workspace/Users/shaunak.basu@perficient.com/formula_one_project/Formula_One_Project/includes/configuration"

# COMMAND ----------

data_path = presentation_folder_path

#Read in data

races_df = spark.read.parquet(f"{data_path}/races_results")

# COMMAND ----------

#Filter by year
year = "2020"
races_df_year = races_df.filter(f"race_year == {year}")
display(races_df_year)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, desc

races_per_year = races_df.select(countDistinct("race_name"))
races_per_year.show()

# COMMAND ----------

#Grouping by number of races per year
#countDistinct needs the aggegrate function.

#If you want to use multiple aggrgate functions,such as sum and count on the same dataframe, grouped on the 
#same column, you need to use the agg function.

races_df.groupBy("race_year").agg(countDistinct("race_name")).orderBy("race_year",ascending=False).show(100)

# COMMAND ----------

#Group dataframe for year 2020, by driver name and total points

races_df_year = races_df.filter(f"race_year == {year}")

races_driver_points = races_df_year.groupBy("driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")).orderBy(desc("total_points")).show()

# COMMAND ----------

#Give each driver a rank based on the total points, here we use the window function on the year

race_position_by_year = races_df.groupBy("driver_name","race_year") \
  .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
  .orderBy(desc("race_year"), desc("total_points"))


# COMMAND ----------


#Provide a rank to each driver based on total points, per year
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

rank_order = Window.partitionBy("race_year").orderBy(desc("total_points"))

ranked_position_by_year = race_position_by_year.withColumn("rank", rank().over(rank_order)).orderBy(desc("race_year"))

display(ranked_position_by_year)

# COMMAND ----------

#Rank drivers based on total number of wins
from pyspark.sql.functions import when, col

race_position_order_by_year = races_df.groupBy("driver_name","race_year","driver_nationality","team") \
  .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"), \
  count(when(col("position") == 1, True)).alias("number_of_wins")) \
  .orderBy(desc("race_year"), desc("total_points"), desc("number_of_wins"))
  
display(race_position_order_by_year)

# COMMAND ----------

#provide rank for the standings above

window_rank = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("number_of_wins"))

final_rank_points_wins = race_position_order_by_year.withColumn("rank", rank().over(window_rank)).orderBy(desc("race_year"))

display(final_rank_points_wins)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Providing the ranking of each team, based on the total number of points####

# COMMAND ----------

team_order = races_df.groupBy("team", "race_year") \
  .agg(sum("points").alias("total_points"), \
    count(when(col("position") == 1, True)).alias("number_of_wins")) \
   .orderBy(desc("race_year"), desc("total_points"), desc("number_of_wins"))

display(team_order) 

# COMMAND ----------

#Providing rank for the teams

rank_order = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("number_of_wins"))

final_team_ranks = team_order.withColumn("rank", rank().over(rank_order)).orderBy(desc("race_year"))

display(final_team_ranks)
