# README

This repository contains following files:
+ **data_analysis.py** - main file on databricks where I analysed collected data

+ **data_analysis.ipynb** - jupyter notebook with analysis that was generated from *data_analysis.py*.
*Look in this notebook to see the results*.

+ **data_collection.py** - notebook that was run regularly to collect data from kafka

+ **pid_schema** - schema of collected data
 
### Assignment 2 – Locations with the greatest decrease in delays

From the data stream, implement a stream processing application that will monitor the delay
of traffic -> where delays are fastest decreasing. Detect the locations where the most traffic 
"spikes" occur repeatedly.

Input: Stream
Output: GPS coordinates of the "fastest delay minimization" locations, dashboard map 
showing these locations