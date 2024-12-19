from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col,split,lit,broadcast


# Create a SparkSession
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

# Disable broadcast joins
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# for SPJ to be enabled it should have minimal of these 2 configs set:
# Setting SPJ related configs
spark.conf.set('spark.sql.sources.v2.bucketing.enabled','true') 
spark.conf.set('spark.sql.sources.v2.bucketing.pushPartValues.enabled','true')
spark.conf.set('spark.sql.iceberg.planning.preserve-data-grouping','true')
spark.conf.set('spark.sql.requireAllClusterKeysForCoPartition','false')
spark.conf.set('spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled','true')


# Read the match_details CSV file
match_details = spark.read.option("header", "true").csv("/home/iceberg/data/match_details.csv")

# Read the match CSV file
matches = spark.read.option("header", "true").csv("/home/iceberg/data/matches.csv")

# Read the medals_matches_players CSV file
medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")

# Read the medals CSV file
medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")


# Read the maps CSV file
maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")

# Creating the Bootcamp Database
spark.sql("""
CREATE DATABASE IF NOT EXISTS bootcamp
""")


## Dropping the match_details if exists schema 
spark.sql("""
DROP TABLE IF EXISTS bootcamp.match_details_bucketed
""")


#Creating the match_details schema  using iceberg data format 

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
     match_id STRING,
     player_gamertag STRING,
     player_total_kills INTEGER,
     player_total_deaths INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id))

""")


#Writing the match_details data into the match_details spark table 

match_details.select("match_id", "player_gamertag","player_total_kills", "player_total_deaths") \
.write.mode("overwrite") \
.bucketBy(16, "match_id").saveAsTable("bootcamp.match_details_bucketed") 
  

print(f"Match details records count : {match_details.count()}")


## Dropping the matches schema 

spark.sql("""
DROP TABLE IF EXISTS bootcamp.matches_bucketed
""")

#Creating the medals schema using iceberg data format 

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
        match_id STRING,
        mapid STRING,
        is_team_game STRING,
        playlist_id STRING,
        completion_date TIMESTAMP
  )
   USING iceberg
   PARTITIONED BY (bucket(16, match_id))
   """)

#Writing the matches data into the matches spark table 

matches.select("match_id", "mapid", "is_team_game", "is_match_over", "playlist_id", "completion_date") \
.write.mode("overwrite") \
.bucketBy(16, "match_id").saveAsTable("bootcamp.matches_bucketed") 
  

print(f"Matches records count : {matches.count()}")


## Dropping the medals_matches_players schema 

spark.sql("""
DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed
""")

#Creating the medals_matches_players schema using iceberg data format 

spark.sql("""
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
        match_id STRING,
        player_gamertag STRING,
        medal_id STRING,
        count INTEGER
  )
   USING iceberg
   PARTITIONED BY (bucket(16, match_id));
""")


# Writing into the  medals_matches_players Schema
medals_matches_players.select("match_id", "player_gamertag","medal_id", "count",) \
     .write.mode("overwrite") \
     .bucketBy(16, "match_id").saveAsTable("bootcamp.medals_matches_players_bucketed") 

print(f"Medals Match Player details records count : {medals_matches_players.count()}")


# Save the Medals DataFrame as a table
medals.write.mode("overwrite").saveAsTable("bootcamp.medals")
# medals.createOrReplaceTempView("bootcamp.medals")

# Save the Maps DataFrame as a table
maps.write.mode("overwrite").saveAsTable("bootcamp.maps")
# maps.createOrReplaceTempView("bootcamp.maps")


#Bucket join and broadcast join to get merged aggregate data frame to answer questions
spark.table("bootcamp.matches_bucketed").alias("mtc") \
.join(spark.table("bootcamp.match_details_bucketed").alias("mth_det"), col("mtc.match_id") == col("mth_det.match_id"), "left") \
.join(spark.table("bootcamp.medals_matches_players_bucketed").alias("medals_mth_players"), ((col("mtc.match_id") == col("medals_mth_players.match_id")) &
(col("mth_det.player_gamertag") == col("medals_mth_players.player_gamertag"))), "left") \
.join(broadcast(spark.table("bootcamp.medals")).alias("medls"), col("medals_mth_players.medal_id") == col("medls.medal_id"), "inner") \
.join(broadcast(spark.table("bootcamp.maps")).alias("mps"), col("mtc.mapid") == col("mps.mapid"), "inner") \
.select(col("mtc.match_id"),col("mtc.mapid"),col("mps.name").alias("map_name"),col("medals_mth_players.medal_id"),
col("medls.name").alias("medal_name"),col("mtc.playlist_id"),col("mtc.completion_date"), \
col("mth_det.player_gamertag"), col("classification"), col("mth_det.player_total_kills"),col("medals_mth_players.count")).write.mode("overwrite").saveAsTable("bootcamp.final_data")


# viewing the final aggregated merged data
spark.sql("""
select * from bootcamp.final_data
""")

# Question: Which player averages the most kills per game?
# Query- Answer
spark.sql("""
SELECT player_gamertag, match_id, Avg(player_total_kills) AS kills
FROM bootcamp.final_data
GROUP BY player_gamertag, match_id
ORDER BY kills DESC
LIMIT(1)
""")

# Question: Which playlist gets played the most?
# Query- Answer
spark.sql("""
SELECT playlist_id, COUNT(Distinct match_id) AS count
FROM bootcamp.final_data
GROUP BY playlist_id
ORDER BY count DESC
LIMIT(1)
""")


# Question: Which map gets played the most?
# Query- Answer
spark.sql(""" 
SELECT map_name,count(distinct match_id) AS count
FROM bootcamp.final_data
GROUP BY map_name
ORDER BY count DESC
LIMIT(1)
""")

# Question: Which map do players get the most Killing Spree medals on?
# Query- Answer
spark.sql("""   
SELECT map_name,Sum(Count) AS count
FROM bootcamp.final_data
WHERE classification = "KillingSpree"
GROUP BY map_name
ORDER BY count DESC
LIMIT(1)
""")


#reading the final data back into dataframe
final_df = spark.read.table("bootcamp.final_data")


##Trying different partition columns and number to see which has the smallest data size
#local partition sort
sorted = final_df.repartition(5, col("match_id")) \
                .sortWithinPartitions(col("playlist_id"),col("map_name"),col("match_id"))


#global sort 
unsorted = final_df.repartition(5, col("match_id")) \
            .sort(col("playlist_id"),col("map_name"),col("match_id"))


# sorted.show(5)
# unsorted.show(5)
sorted.explain()
unsorted.explain()
sorted.write.mode("overwrite").saveAsTable("bootcamp.finaldf_sorted")
unsorted.write.mode("overwrite").saveAsTable("bootcamp.finaldf_unsorted")


#Files size after and before sort within partioning 
spark.sql("""
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
FROM bootcamp.finaldf_sorted.files

UNION ALL
SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
FROM bootcamp.finaldf_unsorted.files
""")




