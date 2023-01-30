# Databricks notebook source
# MAGIC %md
# MAGIC ### BDT Spark Streaming - HW02

# COMMAND ----------

# MAGIC %md
# MAGIC #### Assignment
# MAGIC 
# MAGIC From the data stream, implement a stream processing application that will monitor the delay
# MAGIC of traffic -> where delays are fastest decreasing. Detect the locations where the most traffic 
# MAGIC "spikes" occur repeatedly.
# MAGIC 
# MAGIC Input: Stream
# MAGIC Output: GPS coordinates of the "fastest delay minimization" locations, dashboard map 
# MAGIC showing these locations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data selection
# MAGIC 
# MAGIC I chose to process only data from trams stram, but pretty much the same approach could be applied on other vehicle types. Either separately, or tables could be merged and processed all together.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS trams_delays;
# MAGIC CREATE TABLE trams_delays AS
# MAGIC SELECT 
# MAGIC   ROW_NUMBER() OVER(PARTITION BY trip_id ORDER BY timestamp ASC) as trip_msg_order,
# MAGIC   *
# MAGIC FROM
# MAGIC (SELECT 
# MAGIC   cast(properties.last_position.origin_timestamp as timestamp) as timestamp,
# MAGIC   properties.trip.gtfs.trip_id as trip_id,
# MAGIC   cast(geometry.coordinates[0] as double) AS x,
# MAGIC   cast(geometry.coordinates[1] as double) AS y,
# MAGIC   properties.last_position.delay.actual as delay
# MAGIC FROM trams);
# MAGIC 
# MAGIC -- This query assigns order in which each message was generated for each trip. And also selects crutial information that will be needed later on.
# MAGIC -- I do this so I can compute delay decrement between two consecutive locations for each trip.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*) as count,
# MAGIC   MIN(delay_delta) as min_delta,
# MAGIC   x, y
# MAGIC FROM
# MAGIC   (SELECT
# MAGIC       T2.x, T2.y,
# MAGIC       (T2.delay - T1.delay) AS delay_delta 
# MAGIC   FROM
# MAGIC   trams_delays T1, trams_delays T2
# MAGIC   WHERE (T1.trip_msg_order + 1) = T2.trip_msg_order and T1.trip_id = T2.trip_id
# MAGIC   ORDER BY delay_delta ASC)
# MAGIC WHERE delay_delta < -10       -- i.e. select locations where delay decreased and set arbitrary threshold (at least 10s improvement in delay)
# MAGIC GROUP BY x, y
# MAGIC ORDER BY count DESC
# MAGIC LIMIT 3;
# MAGIC 
# MAGIC -- In this query delay improvement between each recorded point of each trip is calculated. 
# MAGIC -- Then only top three places where delay decreased most often (as specified in assignment) are selected

# COMMAND ----------

# save results to pandas dataframe
df_results = _sqldf.toPandas()

# COMMAND ----------

import osmnx as ox

custom_filter='["highway"~"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|road|road_link"]'
G = ox.graph_from_place("Praha, Czechia", custom_filter=custom_filter)

# COMMAND ----------

# MAGIC %md
# MAGIC Locations of the most frequent delay minimization are plotted on the map. Here, two points are very close to each other, and they overlap. But overall, all three points are on the map.

# COMMAND ----------

# plot results on map

import matplotlib.pyplot as plt
fig, ax = ox.plot_graph(G, show=False, close=False, node_size=0)
x = df_results['x']
y = df_results['y']
ax.scatter(x, y, c='red', alpha=.7)
plt.show()
