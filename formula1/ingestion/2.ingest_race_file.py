# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest race.csv file##

# COMMAND ----------

races_df = spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/mnt/formulagroup2/raw/races.csv")

# COMMAND ----------

races_df.printSchema

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                  StructField("year",IntegerType(),True),
                                  StructField("round",IntegerType(),True),
                                  StructField("circuitId",IntegerType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("date",DateType(),True),
                                  StructField("time",StringType(),True),
                                  StructField("url",StringType(),True)
                                 ])

# COMMAND ----------

races_df= spark.read.option("header",True).schema(races_schema).csv("dbfs:/mnt/formulagroup2/raw/races.csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,concat,col,lit,to_timestamp

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date",current_timestamp())\
                                   .withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))


# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_Id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_Id'),col('name'),col('ingestion_date'),col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write the output to processed container in parquet format

# COMMAND ----------

races_final_df = races_selected_df.write.mode('overwrite').parquet('/mnt/formulagroup2/processed/races')

# COMMAND ----------


