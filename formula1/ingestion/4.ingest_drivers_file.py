# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file##

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DateType

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename",StringType(),True),
                                   StructField("surname",StringType(),True)           
                                  ])

# COMMAND ----------

drivers_schema = StructType(fields = [StructField("driverId",IntegerType(),False),
                                    StructField("driverRef",StringType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("code",StringType(),True),
                                    StructField("name",name_schema),
                                    StructField("dob",DateType(),True),
                                    StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)
                                    ])

# COMMAND ----------

drivers_df = spark.read\
.schema(drivers_schema)\
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import col,concat,current_timestamp,lit

# COMMAND ----------

drivers_with_columns_df= add_ingestion_date(drivers_df.withColumnRenamed("driverId","driver_id")\
                                 .withColumnRenamed("driverRef","driver_ref")\
                                 .withColumn("ingestion_date",current_timestamp())\
                                 .withColumn("name", concat(col("name.forename"),lit(" "),col("name.surname")))
                                 .withColumn("p_data_source",lit(v_data_source)))

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/driver")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/driver"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


