# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest race.csv file##

# COMMAND ----------

dbutils.widgets.text("p_data_sorce","")
v_data_source = dbutils.widgets.get("p_data_sorce")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.option("header",True).option("inferSchema",True).csv(f"{raw_folder_path}/races.csv")

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

races_df= spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,concat,col,lit

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
                                   .withColumn("data_source",lit(v_data_source)))


# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_Id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_Id'),col('name'),col('ingestion_date'),col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write the output to processed container in parquet format

# COMMAND ----------

races_final_df = races_selected_df.write.mode('overwrite').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


