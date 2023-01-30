# Databricks notebook source
# MAGIC %run "./pid_schema"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC I read all the topics just in case I would need them all. I save data to tables with appropriate topic name. This notebook is run periodically to collect data once in a while.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# connect to broker
JAAS = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="fel.student" password="FelBigDataWinter2022bflmpsvz";'

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

# COMMAND ----------

df_trains = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "trains") \
  .load()

select_base_trains = df_trains.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \

# append data to trains table and save checkpoint
select_stream = select_base_trains.writeStream \
        .trigger(once=True) \
        .format("parquet") \
        .queryName("trains") \
        .outputMode("append")\
        .option("checkpointLocation", "./checkpoints/trains") \
        .toTable("trains")

# COMMAND ----------

df_buses = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "buses") \
  .load()


select_base_buses = df_buses.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \

select_stream = select_base_buses.writeStream \
        .trigger(once=True) \
        .format("parquet") \
        .queryName("buses") \
        .outputMode("append")\
        .option("checkpointLocation", "./checkpoints/buses") \
        .toTable("buses")

# COMMAND ----------

df_trams = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "trams") \
  .load()

select_base_trams = df_trams.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \

select_stream = select_base_trams.writeStream \
        .trigger(once=True) \
        .format("parquet") \
        .queryName("trams") \
        .outputMode("append")\
        .option("checkpointLocation", "./checkpoints/trams") \
        .toTable("trams")

# COMMAND ----------

df_regbuses = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "regbuses") \
  .load()

select_base_regbuses = df_regbuses.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \

select_stream = select_base_regbuses.writeStream \
        .trigger(once=True) \
        .format("parquet") \
        .queryName("regbuses") \
        .outputMode("append")\
        .option("checkpointLocation", "./checkpoints/regbuses") \
        .toTable("regbuses")

# COMMAND ----------

df_boats = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "boats") \
  .load()

select_base_boats = df_boats.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \

select_stream = select_base_boats.writeStream \
        .trigger(once=True) \
        .format("parquet") \
        .queryName("boats") \
        .outputMode("append")\
        .option("checkpointLocation", "./checkpoints/boats") \
        .toTable("boats")
