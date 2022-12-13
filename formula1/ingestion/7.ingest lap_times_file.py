# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest lap_times folder##

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

lap_times_schema  = StructType(fields=[StructField("raceId",IntegerType(),False),
                                      StructField("driverId",IntegerType(),False),
                                      StructField("lap",IntegerType(),True),
                                      StructField("position",IntegerType(),True),
                                      StructField("time",StringType(),True),
                                      StructField("milliseconds",IntegerType(),True)             
                                      ])

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv("/mnt/formulagroup2/raw/lap_times")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId","driver_id")\
                       .withColumnRenamed("raceId","race_id")\
                       .withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formulagroup2/processed/lap_times")

# COMMAND ----------


