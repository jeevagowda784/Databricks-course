# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructor.json file##

# COMMAND ----------

constructor_schema = "constructorId INT,constructorRef STRING, name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructor_df = spark.read\
.schema(constructor_schema)\
.json("/mnt/formulagroup2/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##drop  unwanted column##

# COMMAND ----------

constructor_dropped_df=constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                              .withColumnRenamed("constructor_Ref","constructor_ref")\
                                               .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write output on parquet file##

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formulagroup2/processed/constructors.json")

# COMMAND ----------


