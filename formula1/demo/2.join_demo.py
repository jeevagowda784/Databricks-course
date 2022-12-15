# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.filter("circuit_id<70")\
.withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")\
.withColumnRenamed("name","race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_Id,"inner")\
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)


# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Outer Joins##

# COMMAND ----------

# MAGIC %md
# MAGIC #left outer join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_Id,"left")\
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

 

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_Id,"right")\
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

 

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_Id,"full")\
.select(circuits_df.circuit_name,circuits_df.location,circuits_df.country,races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Semi Joins##

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_Id,"semi")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df,circuits_df.circuit_id == races_df.circuit_Id,"anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

int(races_df.count())*int(circuits_df.count())

# COMMAND ----------


