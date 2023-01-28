# Databricks notebook source
# MAGIC %run "./pid_schema"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col

# connect to broker
JAAS = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="fel.student" password="FelBigDataWinter2022bflmpsvz";'

# COMMAND ----------

df_trains = spark.readStream \
  .format("kafka")\
  .option("kafka.bootstrap.servers", "b-2-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196, b-1-public.bdffelkafka.3jtrac.c19.kafka.us-east-1.amazonaws.com:9196") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512")\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", JAAS) \
  .option("subscribe", "trains") \
  .load()

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_trains = df_trains.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
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

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_buses = df_buses.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
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
#.option("startingTimestamp", stream_start_timestamp) \

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_trams = df_trams.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
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
  .option("subscribe", "buses") \
  .load()

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_regbuses = df_regbuses.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
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

#get schema for the stream from the function in helper notebook
schema_pid=get_pid_schema() 

select_base_boats = df_boats.select(from_json(col("value").cast("string"),schema_pid).alias("data")).select("data.*") \
#lets start reading from the stream stream over casted to memory, be advised, you can ran out of it
#with option .outputMode("append") we are saving only the new data coming to the stream
#with option checkpoint, so the stream knows not to overwrite someother stream, in case we stream the same topics into two streams
#for saving into table we can add command .toTable("nameofthetable") , table will be stored in Data>hive_metastore>default>nameofthetable, this may prove usefull for some of you maybe
select_stream = select_base_boats.writeStream \
        .trigger(once=True) \
        .format("parquet") \
        .queryName("boats") \
        .outputMode("append")\
        .option("checkpointLocation", "./checkpoints/boats") \
        .toTable("boats")
