# Databricks notebook source
# MAGIC %md ## Socialflow All

# COMMAND ----------

daily_outs_facebook_all = spark.read.format("delta").load("s3://cn-falcon/analysis/socialflow_daily_facebook_page_all").drop("Total_FB_Clicks", "FB_Link_Clicks")
daily_outs_twitter_all = spark.read.format("delta").load("s3://cn-falcon/analysis/socialflow_daily_twitter_all")

# COMMAND ----------

# display(daily_outs_twitter_all)

# COMMAND ----------

combined_daily_all = daily_outs_facebook_all.union(daily_outs_twitter_all)

# COMMAND ----------

# display(combined_daily_all)

# COMMAND ----------

combined_daily_all.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://cn-falcon/analysis/analysis_socialflow_daily_all")

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_socialflow_daily_all;
# CREATE TABLE falcon.analysis_socialflow_daily_all
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/analysis_socialflow_daily_all"

# COMMAND ----------

# MAGIC %md ## Socialflow Older

# COMMAND ----------

daily_outs_facebook_older = spark.read.format("delta").load("s3://cn-falcon/analysis/socialflow_daily_facebook_page_older").drop("Total_FB_Clicks", "FB_Link_Clicks")
daily_outs_twitter_older = spark.read.format("delta").load("s3://cn-falcon/analysis/socialflow_daily_twitter_older")

# COMMAND ----------

combined_daily_older = daily_outs_facebook_older.union(daily_outs_twitter_older)

# COMMAND ----------

combined_daily_older.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://cn-falcon/analysis/analysis_socialflow_daily_older")

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_socialflow_daily_older;
# CREATE TABLE falcon.analysis_socialflow_daily_older
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/analysis_socialflow_daily_older"

# COMMAND ----------

# MAGIC %md ## GA Distinct URLs

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

ga_distinct_facebook_urls = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_distinct_social_facebook_page_urls").withColumn("social_account_type", F.lit("facebook_page"))
ga_distinct_twitter_urls = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_distinct_social_twitter_urls").withColumn("social_account_type", F.lit("twitter"))

# COMMAND ----------

ga_distinct_all = ga_distinct_facebook_urls.union(ga_distinct_twitter_urls)

# COMMAND ----------

ga_distinct_all.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://cn-falcon/analysis/analysis_ga_distinct_social_urls")

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_ga_distinct_social_urls;
# CREATE TABLE falcon.analysis_ga_distinct_social_urls
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/analysis_ga_distinct_social_urls"

# COMMAND ----------

# MAGIC %md ## GA Organic Social Traffic

# COMMAND ----------

# MAGIC %md ### ALL

# COMMAND ----------

ga_social_traffic_fb_all = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_organic_social_pageviews_sessions_facebook_page_all").withColumn("social_account_type", F.lit("facebook_page")).withColumnRenamed("facebook_page_sessions", "social_account_sessions").withColumnRenamed("facebook_page_pageviews", "social_account_pageviews")
ga_social_traffic_tw_all = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_organic_social_pageviews_sessions_twitter_all").withColumn("social_account_type", F.lit("twitter")).withColumnRenamed("twitter_sessions", "social_account_sessions").withColumnRenamed("twitter_pageviews", "social_account_pageviews")

# COMMAND ----------

ga_social_traffic_all = ga_social_traffic_fb_all.union(ga_social_traffic_tw_all)

# COMMAND ----------

ga_social_traffic_all.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://cn-falcon/analysis/analysis_ga_organic_social_traffic_all")

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_ga_organic_social_traffic_all;
# CREATE TABLE falcon.analysis_ga_organic_social_traffic_all
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/analysis_ga_organic_social_traffic_all"

# COMMAND ----------

# MAGIC %md ## OLDER

# COMMAND ----------

ga_social_traffic_fb_older = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_organic_social_pageviews_sessions_facebook_page_older").withColumn("social_account_type", F.lit("facebook_page")).withColumnRenamed("facebook_page_sessions", "social_account_sessions").withColumnRenamed("facebook_page_pageviews", "social_account_pageviews")
ga_social_traffic_tw_older = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_organic_social_pageviews_sessions_twitter_older").withColumn("social_account_type", F.lit("twitter")).withColumnRenamed("twitter_sessions", "social_account_sessions").withColumnRenamed("twitter_pageviews", "social_account_pageviews")

# COMMAND ----------

ga_social_traffic_older = ga_social_traffic_fb_older.union(ga_social_traffic_tw_older)

# COMMAND ----------

ga_social_traffic_older.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://cn-falcon/analysis/analysis_ga_organic_social_traffic_older")

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_ga_organic_social_traffic_older;
# CREATE TABLE falcon.analysis_ga_organic_social_traffic_older
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/analysis_ga_organic_social_traffic_older"

# COMMAND ----------

# MAGIC %md ## GA Window Traffic

# COMMAND ----------

# MAGIC %md ### ALL

# COMMAND ----------

ga_window_traffic_fb_all = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_window_pageviews_sessions_facebook_page_all").withColumn("social_account_type", F.lit("facebook_page")).withColumnRenamed("facebook_page_vq_sessions", "social_account_vq_sessions").withColumnRenamed("facebook_page_vq_pageviews", "social_account_vq_pageviews")
ga_window_traffic_tw_all = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_window_pageviews_sessions_twitter_all").withColumn("social_account_type", F.lit("twitter")).withColumnRenamed("twitter_vq_sessions", "social_account_vq_sessions").withColumnRenamed("twitter_vq_pageviews", "social_account_vq_pageviews")

# COMMAND ----------

# display(ga_window_traffic_fb_all)

# COMMAND ----------

# display(ga_window_traffic_tw_all)

# COMMAND ----------

ga_window_traffic_all = ga_window_traffic_fb_all.union(ga_window_traffic_tw_all)

# COMMAND ----------

ga_window_traffic_all.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://cn-falcon/analysis/analysis_ga_window_traffic_all")

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_ga_window_traffic_all;
# CREATE TABLE falcon.analysis_ga_window_traffic_all
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/analysis_ga_window_traffic_all"

# COMMAND ----------

# MAGIC %md ### OLDER

# COMMAND ----------

ga_window_traffic_fb_older = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_window_pageviews_sessions_facebook_page_older").withColumn("social_account_type", F.lit("facebook_page")).withColumnRenamed("facebook_page_vq_sessions", "social_account_vq_sessions").withColumnRenamed("facebook_page_vq_pageviews", "social_account_vq_pageviews")
ga_window_traffic_tw_older = spark.read.format("delta").load("s3://cn-falcon/analysis/ga_window_pageviews_sessions_twitter_older").withColumn("social_account_type", F.lit("twitter")).withColumnRenamed("twitter_vq_sessions", "social_account_vq_sessions").withColumnRenamed("twitter_vq_pageviews", "social_account_vq_pageviews")

# COMMAND ----------

ga_window_traffic_older = ga_window_traffic_fb_older.union(ga_window_traffic_tw_older)

# COMMAND ----------

ga_window_traffic_older.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://cn-falcon/analysis/analysis_ga_window_traffic_older")

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_ga_window_traffic_older;
# CREATE TABLE falcon.analysis_ga_window_traffic_older
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/analysis_ga_window_traffic_older"

# COMMAND ----------


