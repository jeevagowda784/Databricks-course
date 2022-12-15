# Databricks notebook source
v_results = dbutils.notebook.run("1.ingestion_circuits_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("2.ingest_race_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("3.ingest_constructor_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("4.ingest_drivers_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("5.ingest_result_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("6.ingest _pit_stops_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("7.ingest lap_times_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results = dbutils.notebook.run("8.ingest_qualifying_file",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

v_results

# COMMAND ----------


