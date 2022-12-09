# Databricks notebook source
# MAGIC %md
# MAGIC ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC step-1 Read the CSV file using spark Data frame reader

# COMMAND ----------

circuits_df=spark.read.option("header",True).csv("dbfs:/mnt/formulagroup2/raw/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df.show())

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

circuits_df=spark.read\
.option("header",True)\
.o("inferSchema",True)\
.csv("dbfs:/mnt/formulagroup2/raw/circuits.csv")

# COMMAND ----------

circuits_df=spark.read\
.option("header",True)\
.option("inferSchema",True)\
.csv("dbfs:/mnt/formulagroup2/raw/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                   StructField("circuitRef",StringType(),False),
                                   StructField("name",StringType(),False),
                                   StructField("location",StringType(),False),
                                   StructField("country",StringType(),False),
                                   StructField("lat",DoubleType(),False),
                                   StructField("lng",DoubleType(),False),
                                   StructField("alt",IntegerType(),False),
                                   StructField("url",StringType(),False),
                           
                           ])                           

# COMMAND ----------

circuits_df=spark.read\
.option("header",True)\
.schema(circuits_schema)\
.csv("dbfs:/mnt/formulagroup2/raw/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------


