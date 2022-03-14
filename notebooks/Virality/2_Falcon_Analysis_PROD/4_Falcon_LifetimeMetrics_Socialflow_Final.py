# Databricks notebook source
# dbutils.widgets.dropdown("brand_name", "self", ['self', 
#                                                 'teen-vogue', 
#                                                 'conde-nast-traveler', 
#                                                 'allure', 
#                                                 'architectural-digest', 
#                                                 'vanity-fair', 
#                                                 'epicurious', 
#                                                 'vogue', 
#                                                 'pitchfork', 
#                                                 'gq', 
#                                                 'wired', 
#                                                 'new-yorker', 
#                                                 'glamour', 
#                                                 'bon-appetit',
#                                                 'vogue-uk',
#                                                 'gq-uk',
#                                                 'them'
#                                                ])
# dbutils.widgets.dropdown("social_platform_name", "facebook_page", ["facebook_page", "twitter"])
# dbutils.widgets.dropdown("social_partner_name", "socialflow", ["socialflow", "slack"])
# dbutils.widgets.dropdown("mode", "dev", ['dev', 'prod'])

# COMMAND ----------

# MAGIC %run ./3_Falcon_Analysis_Socialdata_Kafka_Base

# COMMAND ----------

s_obj.brand_posteddata_ids

# COMMAND ----------

# socialflow_content_df_with_time = spark.table("falcon.analysis_socialflow_social_{}_complete".format(social_platform_name))
socialflow_content_df_with_time = spark.read.format("delta").load("s3://cn-dse-falcon-{}/analysis/socialflow_social_{}_complete".format(mode, social_platform_name))

# COMMAND ----------

socialflow_content_df_with_time = socialflow_content_df_with_time.where(F.col("brand") == brand_name)

# COMMAND ----------

# socialflow_content_df_with_time.printSchema()

# COMMAND ----------

# MAGIC %md # Falcon Usability Analysis : Considering All Articles (Newly Posted + Older Articles)

# COMMAND ----------

min_analysis_date = "2019-01-01"
max_analysis_date = pendulum.now(tz="EST").subtract(days=1).to_date_string() 

# COMMAND ----------

socialflow_content_df_with_time = socialflow_content_df_with_time.where(F.col("social_created_date_est") < max_analysis_date)
socialflow_content_df_with_time_older_articles = socialflow_content_df_with_time.where(F.col("socialpublished_hours_from_closest_pubdate") > 504)

# COMMAND ----------

# MAGIC %md # Socialflow Based Analyis Begins

# COMMAND ----------

if social_platform_name == "facebook_page":
  daily_outs_all = socialflow_content_df_with_time.groupBy([F.col("social_created_date_est").alias("social_published_dt"), "is_falcon"]).agg(F.sum("socialflow_clicks").alias("Socialflow_Clicks"), F.sum("total_fb_clicks").alias("Total_FB_Clicks"), F.sum("fb_link_clicks_lifetime").alias("FB_Link_Clicks"), F.countDistinct("copilot_id").alias("Total_Posts")).orderBy("social_published_dt", "is_falcon").withColumn("brand", F.lit(brand_name)).withColumn("social_platform_name", F.lit(social_platform_name))
else:
  daily_outs_all = socialflow_content_df_with_time.groupBy([F.col("social_created_date_est").alias("social_published_dt"), "is_falcon"]).agg(F.sum("socialflow_clicks").alias("Socialflow_Clicks"), F.countDistinct("copilot_id").alias("Total_Posts")).orderBy("social_published_dt", "is_falcon").withColumn("brand", F.lit(brand_name)).withColumn("social_platform_name", F.lit(social_platform_name))

# COMMAND ----------

daily_outs_all.write \
    .format("delta") \
    .partitionBy("brand") \
    .mode("overwrite") \
    .option("replaceWhere", "brand == '{}'".format(brand_name)) \
    .save("s3://cn-dse-falcon-{}/analysis/socialflow_daily_{}_all".format(mode, social_platform_name))

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_socialflow_daily_multiaccounts_all;
# CREATE TABLE falcon.analysis_socialflow_daily_multiaccounts_all
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/socialflow_daily_multiaccounts_ALL"

# COMMAND ----------

# display(daily_outs_all)

# COMMAND ----------

if social_platform_name == "facebook_page":
  daily_outs_older = socialflow_content_df_with_time_older_articles.groupBy([F.col("social_created_date_est").alias("social_published_dt").alias("social_published_dt"), "is_falcon"]).agg(F.sum("socialflow_clicks").alias("Socialflow_Clicks"), F.sum("total_fb_clicks").alias("Total_FB_Clicks"), F.sum("fb_link_clicks_lifetime").alias("FB_Link_Clicks"), F.countDistinct("copilot_id").alias("Total_Posts")).orderBy("social_published_dt", "is_falcon").withColumn("brand", F.lit(brand_name)).withColumn("social_platform_name", F.lit(social_platform_name))
else:
  daily_outs_older = socialflow_content_df_with_time_older_articles.groupBy([F.col("social_created_date_est").alias("social_published_dt").alias("social_published_dt"), "is_falcon"]).agg(F.sum("socialflow_clicks").alias("Socialflow_Clicks"), F.countDistinct("copilot_id").alias("Total_Posts")).orderBy("social_published_dt", "is_falcon").withColumn("brand", F.lit(brand_name)).withColumn("social_platform_name", F.lit(social_platform_name))

# COMMAND ----------

daily_outs_older.write \
    .format("delta") \
    .partitionBy("brand") \
    .mode("overwrite") \
    .option("replaceWhere", "brand == '{}'".format(brand_name)) \
    .save("s3://cn-dse-falcon-{}/analysis/socialflow_daily_{}_older".format(mode, social_platform_name))

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_socialflow_daily_twitter_older;
# CREATE TABLE falcon.analysis_socialflow_daily_twitter_older
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/socialflow_daily_twitter_older"

# COMMAND ----------


