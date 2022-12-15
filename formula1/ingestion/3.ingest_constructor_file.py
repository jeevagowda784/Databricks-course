# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructor.json file##

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructor_schema = "constructorId INT,constructorRef STRING, name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df = spark.read\
.schema(constructor_schema)\
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop  unwanted column##

# COMMAND ----------

constructor_dropped_df=constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                              .withColumnRenamed("constructor_Ref","constructor_ref").withColumn("p_data_source",lit(v_data_source)))
                                               
                                               

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write output on parquet file##

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors.json")

# COMMAND ----------

display(spark.read.parquet("/mnt/formulagroup2/processed/constructors.json"))

# COMMAND ----------

dbutils.notebook.exit("Success")
