# Databricks notebook source
file_location = "/FileStore/Nikhil/Json/data.json"
file_type = "json"

df = spark.read.format(file_type) \
  .option("multiline", True) \
  .load(file_location)

# COMMAND ----------

from pyspark.sql.functions import col, explode
df_exploded = df.select(
    col("country"), explode(col("data")).alias("data"), col("is_end"), col("total")
)

# COMMAND ----------

from pyspark.sql.functions import col, explode
df_flattened = df_exploded.select(
    col("country"),
    col("data.date_time"),
    col("data.Key_in_date"),
    col("data.lastmodified_date"),
    col("data.task_id"),
    col("data.store-id"),
    col("data.member_login_id"),
    col("data.check_in"),
    col("data.check_out"),
    col("data.user_location")[0].alias("user_latitude"),
    col("data.user_location")[1].alias("user_longitude"),
    col("data.distance"),
    explode(col("data.questions")).alias("question"),
    col("total"),
    col("is_end"),
)

# COMMAND ----------

from pyspark.sql.functions import col, explode
df_final = df_flattened.select(
    "country",
    "date_time",
    "Key_in_date",
    "lastmodified_date",
    "task_id",
    "store-id",
    "member_login_id",
    "check_in",
    "check_out",
    "user_latitude",
    "user_longitude",
    "distance",
    col("question.type").alias("question_type"),
    col("question.question").alias("question_text"),
    col("question.question_id").alias("question_id"),
    col("question.answer").alias("answer"),
    "total",
    "is_end"
)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Need to save in paticular location and should call use for flattern json.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F

delta_table = DeltaTable.forName(spark, "silver_table")


merge_condition = "target.task_id = source.task_id AND target.question_id = source.question_id"

delta_table.alias("target") \
    .merge(
        df_final.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

# df_final.write.mode("overwrite").saveAsTable("silver_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold layer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC read data from the file location and do filter on requirement

# COMMAND ----------

from pyspark.sql.functions import col,array
df = spark.read.table("silver_table")
df_product = df.filter(col("question_type") == "product").select("*")

# COMMAND ----------

from pyspark.sql.functions import explode, col, from_json,row_number ,desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType
product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_code", StringType(), True),
    StructField("price_gross", FloatType(), True),
    StructField("price_net", FloatType(), True),
    StructField("price_discount", FloatType(), True),
    StructField("price_incentive", FloatType(), True),
    StructField("sales_unit", IntegerType(), True),
    StructField("sales_gross", FloatType(), True),
    StructField("sales_net", FloatType(), True),
    StructField("promotion", StringType(), True),
    StructField("answer_id", StringType(), True)
])
df_parsed = df_product.withColumn("answer", from_json(col("answer"), ArrayType(product_schema)))
df_exploded = df_parsed.withColumn("answer", explode(col("answer")))


# COMMAND ----------

df_flattened = df_exploded.select(
    "country", "date_time", "Key_in_date", "lastmodified_date", "task_id", 
    "store-id", "member_login_id", "check_in", "check_out", 
    "user_latitude", "user_longitude", "distance", "question_type", 
    "question_text", "question_id", "total", "is_end",
    col("answer.product_id"),
    col("answer.product_code"),
    col("answer.price_gross"),
    col("answer.price_net"),
    col("answer.price_discount"),
    col("answer.price_incentive"),
    col("answer.sales_unit"),
    col("answer.sales_gross"),
    col("answer.sales_net"),
    col("answer.promotion"),
    col("answer.answer_id")
)


# COMMAND ----------

delta_table = DeltaTable.forName(spark, "gold_table")
delta_table.alias("target") \
    .merge(
        df_flattened.alias("source"),
        "target.task_id = source.task_id AND target.question_id = source.question_id AND target.answer_id = source.answer_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .whenNotMatchedBySourceDelete() \
    .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_table

# COMMAND ----------

# MAGIC %md
# MAGIC write the data into gold table

# COMMAND ----------

# df_flattened.write.mode("overwrite").saveAsTable("gold_table")

# COMMAND ----------

# MAGIC %md
# MAGIC read data from the gold table and handle unwanted records.

# COMMAND ----------

# from pyspark.sql.window import Window
# from pyspark.sql.functions import col,row_number,desc
# df = spark.read.table("gold_table")
# window_spec = Window.partitionBy("task_id","question_id").orderBy(col("lastmodified_date").desc())
# df_with_rank = df.withColumn("rank",rank().over(window_spec)).filter(col("row_num") == 1).select("*").drop("rank")
# df_with_rank.display()
