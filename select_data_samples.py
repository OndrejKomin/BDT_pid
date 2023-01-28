# Databricks notebook source
# MAGIC %sql
# MAGIC select count(*) from trams;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from trains;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from buses;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from regbuses;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from boats;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trams limit 1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe trams;

# COMMAND ----------

spark.sql("SELECT properties.last_position.* FROM trams LIMIT 1").toPandas() #.columns

# COMMAND ----------

tables = ['trams', 'buses', 'regbuses', 'trains', 'boats']
limit = str(5000)


for table in tables:
    # properties
    df_last_stop = spark.sql("SELECT properties.last_position.last_stop.* FROM " + table + " LIMIT " + limit).toPandas().rename(columns=lambda x: "lp.last_stop." + x)
    df_next_stop = spark.sql("SELECT properties.last_position.next_stop.* FROM " + table + " LIMIT " + limit).toPandas().rename(columns=lambda x: "lp.next_stop." + x)
    df_delay = spark.sql("SELECT properties.last_position.delay.* FROM " + table + " LIMIT " + limit).toPandas().rename(columns=lambda x: "lp.delay." + x)

    rest_lp_cols = ['bearing', 'is_canceled', 'origin_timestamp', 'shape_dist_traveled', 'speed', 'state_position', 'tracking']
    rest_lp_cols_concat = ", ".join(["properties.last_position."+x for x in rest_lp_cols])
    df_other_last_pos = spark.sql("SELECT " + rest_lp_cols_concat + " FROM " + table + " LIMIT " + limit).toPandas().rename(columns=lambda x: "lp." + x)

    
    # trips
    df_gtfs = spark.sql("SELECT properties.trip.gtfs.* FROM " + table + " LIMIT " + limit).toPandas().rename(columns=lambda x: "pr.gtfs." + x)
    df_veh_type = spark.sql("SELECT properties.trip.vehicle_type.* FROM " + table + " LIMIT " + limit).toPandas().rename(columns=lambda x: "pr.vehicle_type." + x)
    df_cis = spark.sql("SELECT properties.trip.cis.* FROM " + table + " LIMIT " + limit).toPandas().rename(columns=lambda x: "pr.cis." + x)

    rest_pr_cols = ['agency_name', 'origin_route_name', 'sequence_id', 'start_timestamp', 'vehicle_registration_number', 'wheelchair_accessible', 'air_conditioned']
    rest_pr_cols_concat = ", ".join(["properties.trip."+x for x in rest_pr_cols])
    df_other_trip = spark.sql("SELECT " + rest_pr_cols_concat + " FROM " + table + " LIMIT " + limit).toPandas().rename(columns=lambda x: "pr." + x)

    #geometry
    df_coordinates = spark.sql("SELECT geometry.coordinates FROM " + table + " LIMIT " + limit).toPandas()

    
    df_table_samples = df_last_stop.join(df_next_stop).join(df_delay).join(df_other_last_pos).join(df_gtfs).join(df_veh_type).join(df_cis).join(df_other_trip).join(df_coordinates)
    df_table_samples.to_csv("samples/" + table + ".csv", index=False)
