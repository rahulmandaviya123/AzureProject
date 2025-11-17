# Databricks notebook source
# MAGIC %md
# MAGIC ## **AUTOLOADER**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_path = os.path.join(os.getcwd(), '..', '..')
sys.path.append(project_path)

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("cloudFiles.schemaLocation","abfss://silver@datalakerahul123.dfs.core.windows.net/DimUser/checkpoint")\
          .option("schemaEvolutionMode", "addNewColumns")\
          .load("abfss://bronze@datalakerahul123.dfs.core.windows.net/DimUser")
 

# COMMAND ----------

 
df_user = df_user.withColumn("user_name" , upper(col("user_name")))\
          .drop("_rescued_data")\
          .dropDuplicates(['user_id'])

display(df_user)

# COMMAND ----------

df_user.writeStream.format("delta")\
    .option("checkpointLocation", "abfss://silver@datalakerahul123.dfs.core.windows.net/DimUser/checkpoint")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("path","abfss://silver@datalakerahul123.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_catalog.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimArtist**

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("cloudFiles.schemaLocation","abfss://silver@datalakerahul123.dfs.core.windows.net/DimArtist/checkpoint")\
          .option("schemaEvolutionMode", "addNewColumns")\
          .load("abfss://bronze@datalakerahul123.dfs.core.windows.net/DimArtist")
 

# COMMAND ----------

df_artist = df_artist.withColumn("artist_name" , upper(col("artist_name")))\
          .drop("_rescued_data")\
          .dropDuplicates(['artist_id'])

# COMMAND ----------

df_artist.writeStream.format("delta")\
    .option("checkpointLocation", "abfss://silver@datalakerahul123.dfs.core.windows.net/DimArtist/checkpoint")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("path","abfss://silver@datalakerahul123.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("cloudFiles.schemaLocation","abfss://silver@datalakerahul123.dfs.core.windows.net/DimTrack/checkpoint")\
          .option("schemaEvolutionMode", "addNewColumns")\
          .load("abfss://bronze@datalakerahul123.dfs.core.windows.net/DimTrack")

# COMMAND ----------

df_track = df_track.drop("_rescued_data")\
                    .dropDuplicates(['track_id'])

# COMMAND ----------

df_track = df_track.withColumn("durationFlag",when(col("duration_sec") < 150,"Short")\
                              .when(col("duration_sec") < 300,"Medium")\
                              .otherwise("High"))

# COMMAND ----------

df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),"-"," "))

# COMMAND ----------

df_track.writeStream.format("delta")\
    .option("checkpointLocation", "abfss://silver@datalakerahul123.dfs.core.windows.net/DimTrack/checkpoint")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("path","abfss://silver@datalakerahul123.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("cloudFiles.schemaLocation","abfss://silver@datalakerahul123.dfs.core.windows.net/DimDate/checkpoint")\
          .option("schemaEvolutionMode", "addNewColumns")\
          .load("abfss://bronze@datalakerahul123.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date = df_date.drop("_rescued_data")\
                    .dropDuplicates(['date_key'])

# COMMAND ----------

df_date.writeStream.format("delta")\
    .option("checkpointLocation", "abfss://silver@datalakerahul123.dfs.core.windows.net/DimDate/checkpoint")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("path","abfss://silver@datalakerahul123.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_catalog.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC # **FactStream**

# COMMAND ----------

df_stream = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("cloudFiles.schemaLocation","abfss://silver@datalakerahul123.dfs.core.windows.net/FactStream/checkpoint")\
          .option("schemaEvolutionMode", "addNewColumns")\
          .load("abfss://bronze@datalakerahul123.dfs.core.windows.net/FactStream")

# COMMAND ----------

display(df_stream)

# COMMAND ----------

df_stream = df_stream.drop("_rescued_data")\
                    .dropDuplicates(['stream_id'])
 

df_stream.writeStream.format("delta")\
    .option("checkpointLocation", "abfss://silver@datalakerahul123.dfs.core.windows.net/FactStream/checkpoint")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("path","abfss://silver@datalakerahul123.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_catalog.silver.FactStream")