# Databricks notebook source
Data = [(1,"Ram",("2000-01-19 10:20:30")),(2,"Ravi",("1999-05-12 11:20:30")),(3,"Sita",("1998-01-04 05:20:30"))]
df = spark.createDataFrame(Data,["id","name","DOB"])


# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col,split,to_date
df.withColumn("name",split(col("name"),"")).withColumn("DOB",to_date(col("DOB").cast("timestamp"))).display()


