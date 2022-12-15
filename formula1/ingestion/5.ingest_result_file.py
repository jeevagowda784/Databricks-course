# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest result.json file##

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

# COMMAND ----------

result_schema = StructType(fields = [StructField("resultId",IntegerType(),False),
                                     StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),False),
                                     StructField("constructorId",IntegerType(),False),
                                     StructField("number",IntegerType(),True),
                                     StructField("grid",IntegerType(),False),
                                     StructField("position",IntegerType(),True),
                                     StructField("positionText",StringType(),False),
                                     StructField("positionOrder",IntegerType(),False),
                                     StructField("points",FloatType(),False),
                                     StructField("laps",IntegerType(),False),
                                     StructField("time",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True),
                                     StructField("fastestLap",IntegerType(),True),
                                     StructField("rank",IntegerType(),True),
                                     StructField("fastestLapTime",StringType(),True),
                                     StructField("fastestLapSpeed",StringType(),True),
                                     StructField("statusId",IntegerType(),False)])
                                     

# COMMAND ----------

results_df = spark.read\
 .schema(result_schema)\
.json(f"{raw_folder_path}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

results_with_columns_df = add_ingestion_date(results_df.withColumnRenamed("resultId","result_id")\
                                     .withColumnRenamed("raceId","race_id")\
                                     .withColumnRenamed("driverId","driver_id")\
                                     .withColumnRenamed("constructorId","constructor_id")\
                                     .withColumnRenamed("positionText","position_text")\
                                     .withColumnRenamed("positionOrder","position_order")\
                                     .withColumnRenamed("fastestLap","fastest_lap")\
                                     .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                     .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                  .withColumn("ingestion_date",current_timestamp())\
                                  .withColumn("p_data_source",lit(v_data_source)))

# COMMAND ----------

display(results_with_columns_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df  = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(spark.read.parquet("/mnt/formulagroup2/processed/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")
