# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying json files##

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
.json("/mnt/formulagroup2/raw/qualifying")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id")\
.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumnRenamed("colnstructorId","constructor_id")\
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formulagroup2/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formulagroup2/processed/qualifying"))

# COMMAND ----------


