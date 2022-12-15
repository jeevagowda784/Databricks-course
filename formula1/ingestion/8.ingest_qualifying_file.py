# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying json files##

# COMMAND ----------

dbutils.widgets.text("p_data_sorce","")
v_data_source = dbutils.widgets.get("p_data_sorce")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId",IntegerType(),False),
                                        StructField("raceId",IntegerType(),True),
                                         StructField("driverId",IntegerType(),True),
                                         StructField("constructorId",IntegerType(),True),
                                         StructField("number",IntegerType(),True),
                                         StructField("position",IntegerType(),True),
                                         StructField("q1",StringType(),True),
                                        StructField("q2",StringType(),True),
                                        StructField("q3",StringType(),True)
                                        ])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiline",True)\
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_df = add_ingestion_date(qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("colnstructorId","constructor_id")\
.withColumn("p_data_source",lit(v_data_source)))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formulagroup2/processed/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("Success")
