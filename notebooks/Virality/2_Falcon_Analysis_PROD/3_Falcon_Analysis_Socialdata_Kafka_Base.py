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

from falcon.utils.vault_utils import VaultAccess
from falcon.database.sqlalchemy_utils import SQLAlchemyUtils
from falcon.common import read_config, get_min_positive_after_diff

from falcon.socialplatform_utils.socialflow_utils_kafka import Socialflow
from falcon.database.db_utils import DBUtils
from falcon.utils.datetime_utils import DateTimeUtils
from falcon.utils.common_utils import CommonUtils


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import UserDefinedFunction

from sqlalchemy import MetaData, Table, insert, select, update, delete
import re, json
import pandas as pd
import numpy as np
import mlflow
import os
import requests
import pendulum
import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# COMMAND ----------

# %run ../1_Falcon_HourlyRuns_PROD/m_0b_Falcon_Setup_Brands

# COMMAND ----------

brand_name = dbutils.widgets.get("brand_name")
mode = dbutils.widgets.get("mode")
social_platform_name = dbutils.widgets.get("social_platform_name")
social_partner_name = dbutils.widgets.get("social_partner_name")

# COMMAND ----------

vt_rds_data = VaultAccess(mode=mode).get_settings(settings_type="rds")
vt_socialplatform_data = VaultAccess(mode=mode).get_settings(settings_type=f"{social_partner_name}")

# COMMAND ----------

engine = SQLAlchemyUtils().get_engine(vt_rds_data)
metadata_common = MetaData(schema="common")
# metadata_recs = MetaData(schema="recommendations")
metadata_settings = MetaData(schema="settings")
metadata_brand = MetaData(schema="brand")
conn = engine.connect()
# db_utils = DBUtils(conn, engine, metadata_common = metadata_common, metadata_settings = metadata_settings)
db_utils = DBUtils.init_without_db_config(conn, engine, metadata_common = metadata_common, metadata_settings = metadata_settings)

# COMMAND ----------

def read_config_from_postgres(brand_name, platform_name):
  account_id = db_utils.get_social_platform_id(platform_name)
  return db_utils.get_brand_config(brand_name, account_id)

content_config = read_config(settings_name="content")
traffic_config = read_config(settings_name="traffic")
socialcopy_config = read_config(settings_name="socialcopy_data")
falcon_config = read_config("falcon")


prediction_brand_config = read_config_from_postgres(brand_name,social_platform_name)

# COMMAND ----------

aleph_k2d_brandname = prediction_brand_config["brand_alias"]["aleph_k2d"]

# COMMAND ----------

final_content_df = spark.read.format("delta").load(content_config["data"]["s3_bucket"][mode]).where(F.col("brand") == aleph_k2d_brandname)

# COMMAND ----------

# MAGIC %md ## ALL ANALYSIS SCRIPTS HAVE BEEN MOVED TO EST 

# COMMAND ----------

# MAGIC %md ## Define Time Range for Analysis Data

# COMMAND ----------

analysis_start_date = pendulum.datetime(2019, 1, 1, tz="America/New_York")
analysis_end_date = pendulum.now(tz="America/New_York")

# COMMAND ----------

# MAGIC %md ## Load Posted Social Data for the defined Analysis Data time range

# COMMAND ----------

def add_columns_datetime(df, colname_ts):
  df = df.withColumn("created_time_year", F.year(F.col(colname_ts)))
  df = df.withColumn("created_time_month", F.month(F.col(colname_ts)))
  df = df.withColumn("created_time_dow", F.dayofweek(F.col(colname_ts)))
  df = df.withColumn("created_time_hour", F.hour(F.col(colname_ts)))
  df = df.withColumn("created_time_woy", F.weekofyear(F.col(colname_ts)))
  df = df.withColumn("created_time_doy", F.dayofyear(F.col(colname_ts)))
  df = df.withColumn("daytype", F.when(((F.col("created_time_dow") == 1) | (F.col("created_time_dow") == 7)), "weekend").otherwise("weekday"))
  return df

# COMMAND ----------

socialcopy_last_update_time = Table("socialcopy_last_update_time", metadata_brand, autoload=True, autoload_with=engine)
falcon_later_db_table = Table("falcon_later", metadata_brand, autoload=True, autoload_with=engine)
falcon_never_db_table = Table("falcon_never", metadata_brand, autoload=True, autoload_with=engine)
model_outcomes_db_table = Table(f"model_outcomes", metadata_brand, autoload=True, autoload_with=engine)
socialflow_queue_posted_data_db_table = Table(f"socialflow_queue_posted_data", metadata_brand, autoload=True, autoload_with=engine)

social_partner_details = Table("social_partner_details", metadata_common, autoload=True, autoload_with=engine)

db_config = {
              "connection": conn, 
              "engine": engine,
              "socialcopy_db": socialcopy_last_update_time, 
              "social_partner_accounts": social_partner_details, 
              "metadata_common": metadata_common, 
              "later_db": falcon_later_db_table,
              "never_db": falcon_never_db_table,
              "model_outcomes_db": model_outcomes_db_table,
              "socialflow_posted_db": socialflow_queue_posted_data_db_table,
#               "recs_outcomes_db": recs_db_table,
              "metadata_brand": metadata_brand,
              "metadata_settings": metadata_settings
#               "metadata_recs": metadata_recs
            }

s_obj = Socialflow(socialflow_access=vt_socialplatform_data, brand_config=prediction_brand_config, db_config=db_config, social_platform_name=social_platform_name, mode=mode)

# COMMAND ----------

output_list = s_obj._get_posted_data(
            posted_hours=None,
            start_time=analysis_start_date.int_timestamp,
            end_time=analysis_end_date.int_timestamp,
        )
soc_df = spark.read.json(sc.parallelize([json.dumps(output_list)]))

# COMMAND ----------

# display(soc_df)

# COMMAND ----------

# socialcopy_config

# COMMAND ----------

social_df = spark.read.format("delta").load(os.path.join(socialcopy_config["data"]["s3_bucket"][mode], social_platform_name)).where(F.col("brand") == brand_name)
social_df = social_df.where(F.col("social_created_date") < analysis_end_date.to_date_string()).where(F.col("social_created_date") >= analysis_start_date.to_date_string())

# COMMAND ----------

# social_df.count()

# COMMAND ----------

# display(social_df)

# COMMAND ----------

if social_platform_name == "facebook_page":
  joinType = "inner"
  joinExpression = (soc_df["service_message_id"] == social_df["object_id"])
  socialflow_df = soc_df.alias('df1').join(social_df.alias('df2'), joinExpression, joinType).select(['df1.clicks', 
                                                                                                     'df1.comments',
                                                                                                     'df1.fb_post_stats',
                                                                                                     'df1.fb_reactions',
                                                                                                     'df1.likes',
                                                                                                     'df1.shares',
                                                                                                     'df1.reach',
                                                                                                     'df1.meta',
                                                                                                     'df2.*'])    

elif social_platform_name == "twitter":
  joinType = "inner"
  joinExpression = (soc_df["service_message_id"] == social_df["object_id"])
  socialflow_df = soc_df.alias('df1').join(social_df.alias('df2'), joinExpression, joinType).select(['df1.clicks', 
                                                                                                     'df1.meta',
                                                                                                     'df2.*'])    

# COMMAND ----------

final_socialdata_withmetrics_df = socialflow_df

# COMMAND ----------

convert_epochtime_todatetime_str = UserDefinedFunction(DateTimeUtils.convert_epochtime_todatetime_str, StringType())

final_socialdata_withmetrics_df = final_socialdata_withmetrics_df.withColumn("social_created_datetime_est", convert_epochtime_todatetime_str(F.col("social_created_epoch_time"), F.lit("America/New_York")))
final_socialdata_withmetrics_df = final_socialdata_withmetrics_df.withColumn("social_created_date_est", F.col("social_created_datetime_est").cast(DateType()))

# COMMAND ----------

# display(final_socialdata_withmetrics_df)

# COMMAND ----------

# display(final_socialdata_withmetrics_df.where(F.col("social_created_date") != F.col("social_created_date_est")))

# COMMAND ----------

# final_socialdata_withmetrics_df.count()

# COMMAND ----------

# final_socialflow_df.printSchema()

# COMMAND ----------

# final_content_df.printSchema()

# COMMAND ----------

# display(final_content_df)

# COMMAND ----------

final_content_withids_df = final_content_df.select(F.explode("identifier_urls").alias("identifier_urls"), F.col("copilot_id"), F.col("coll_pubdates"), F.col("coll_hfacs"), F.col("channel"), F.col("subchannel"))

# COMMAND ----------

# display(final_content_withids_df)

# COMMAND ----------

joinType = "inner"
joinExpression = final_socialdata_withmetrics_df["cleaned_url"] == final_content_withids_df["identifier_urls"]
final_socialdata_withmetrics_withcontent_df = final_socialdata_withmetrics_df.alias('df1').join(final_content_withids_df.alias('df2'), joinExpression, joinType).select(['df1.*', 'df2.copilot_id', 'df2.coll_pubdates', 'df2.coll_hfacs', 'df2.channel', 'df2.subchannel'])

# COMMAND ----------

# display(final_socialdata_withmetrics_withcontent_df)

# COMMAND ----------

final_socialdata_withmetrics_withcontent_df = final_socialdata_withmetrics_withcontent_df.withColumn("social_created_time_utc", final_socialdata_withmetrics_withcontent_df.social_created_epoch_time.cast(TimestampType()))

# COMMAND ----------

# socialflow_content_df = socialflow_content_df.withColumn("fb_published_datetime", F.from_unixtime(socialflow_content_df["published_date"]))
final_socialdata_withmetrics_withcontent_df_with_time = add_columns_datetime(final_socialdata_withmetrics_withcontent_df, colname_ts="social_created_datetime_est")

# COMMAND ----------

# display(final_socialdata_withmetrics_withcontent_df_with_time)

# COMMAND ----------

@udf("string")
def get_falcon_labels(coll_labels):
  if coll_labels:
    labels = list(coll_labels)
    if "falcon" in labels:
      return "falcon"
    else:
      return "non_falcon"
  else:
    return "non_falcon"

# COMMAND ----------

def make_fb_additional_columns(df):
  socialflow_content_df_with_time = df
  socialflow_content_df_with_time = socialflow_content_df_with_time.withColumn("is_falcon", get_falcon_labels(socialflow_content_df_with_time.meta.getItem("labels")))
  socialflow_content_df_with_time = socialflow_content_df_with_time.withColumn("total_engagement", socialflow_content_df_with_time["shares"] + 
                                                                         socialflow_content_df_with_time["comments"] + 
                                                                         socialflow_content_df_with_time["fb_reactions"].getItem("likes") +
                                                                         socialflow_content_df_with_time["fb_reactions"].getItem("angry") +
                                                                         socialflow_content_df_with_time["fb_reactions"].getItem("haha") +
                                                                         socialflow_content_df_with_time["fb_reactions"].getItem("love") +
                                                                         socialflow_content_df_with_time["fb_reactions"].getItem("sad") +
                                                                         socialflow_content_df_with_time["fb_reactions"].getItem("thankful") +
                                                                         socialflow_content_df_with_time["fb_reactions"].getItem("wow"))


  socialflow_content_df_with_time = socialflow_content_df_with_time.withColumn("total_fb_clicks", socialflow_content_df_with_time["fb_post_stats"].getItem("link_clicks_lifetime") +
                                                                              socialflow_content_df_with_time["fb_post_stats"].getItem("other_clicks_lifetime")
                                                                              )
  socialflow_content_df_with_time = socialflow_content_df_with_time.withColumn("fb_link_clicks_lifetime", socialflow_content_df_with_time.fb_post_stats.getItem("link_clicks_lifetime"))
  socialflow_content_df_with_time = socialflow_content_df_with_time.withColumn("fb_other_clicks_lifetime", socialflow_content_df_with_time.fb_post_stats.getItem("other_clicks_lifetime"))
  socialflow_content_df_with_time = socialflow_content_df_with_time.withColumnRenamed("clicks", "socialflow_clicks")  
  socialflow_content_df_with_time = socialflow_content_df_with_time.drop("meta", "fb_post_stats", "fb_reactions")
  
  return socialflow_content_df_with_time

# COMMAND ----------

def make_twitter_additional_columns(df):
  socialflow_content_df_with_time = df
  socialflow_content_df_with_time = socialflow_content_df_with_time.withColumn("is_falcon", get_falcon_labels(socialflow_content_df_with_time.meta.getItem("labels")))
  socialflow_content_df_with_time = socialflow_content_df_with_time.withColumnRenamed("clicks", "socialflow_clicks")
  socialflow_content_df_with_time = socialflow_content_df_with_time.drop("meta")
  return socialflow_content_df_with_time

# COMMAND ----------

if social_platform_name == "facebook_page":
  final_socialdata_withmetrics_withcontent_df_with_time = make_fb_additional_columns(final_socialdata_withmetrics_withcontent_df_with_time)
elif social_platform_name == "twitter":
  final_socialdata_withmetrics_withcontent_df_with_time = make_twitter_additional_columns(final_socialdata_withmetrics_withcontent_df_with_time)

# COMMAND ----------

from itertools import chain

mapping = {1: "Sunday",
           2: "Monday",
           3: "Tuesday",
           4: "Wednesday",
           5: "Thursday",
           6: "Friday",
           7: "Saturday"
          }
mapping_expr = F.create_map([F.lit(x) for x in chain(*mapping.items())])
final_socialdata_withmetrics_withcontent_df_with_time = final_socialdata_withmetrics_withcontent_df_with_time.withColumn("created_time_dow_alpha", mapping_expr.getItem(final_socialdata_withmetrics_withcontent_df_with_time["created_time_dow"]))

# COMMAND ----------

# display(final_socialdata_withmetrics_withcontent_df_with_time.where(F.col("is_falcon") == "falcon"))

# COMMAND ----------

final_socialdata_withmetrics_withcontent_df_with_time = CommonUtils.get_definedhours_from_anchorpoint_pyspark(final_socialdata_withmetrics_withcontent_df_with_time, output_colname="socialpublished_hours_from_anchor_point", to_ts_col="social_created_time_utc", anchor_ts=traffic_config["anchor_date"])

# COMMAND ----------

# display(final_socialdata_withmetrics_withcontent_df_with_time)

# COMMAND ----------

## Need to add these for twitter also, so can generalize
# final_socialdata_withmetrics_withcontent_df_with_time = final_socialdata_withmetrics_withcontent_df_with_time.withColumn("socialpublished_hours_from_anchor_point1", ((final_socialdata_withmetrics_withcontent_df_with_time.social_created_epoch_time - F.unix_timestamp(F.lit(traffic_config["sparrow"]["anchor_date"]).cast(TimestampType())))/3600).cast(IntegerType()))
get_min_positive_after_diff = UserDefinedFunction(get_min_positive_after_diff, IntegerType())


final_socialdata_withmetrics_withcontent_df_with_time = final_socialdata_withmetrics_withcontent_df_with_time.withColumn("socialpublished_hours_from_closest_pubdate", get_min_positive_after_diff(final_socialdata_withmetrics_withcontent_df_with_time["coll_hfacs"], final_socialdata_withmetrics_withcontent_df_with_time["socialpublished_hours_from_anchor_point"]))
final_socialdata_withmetrics_withcontent_df_with_time = final_socialdata_withmetrics_withcontent_df_with_time.where(F.col("socialpublished_hours_from_closest_pubdate") >= 0)

# COMMAND ----------

# display(final_socialdata_withmetrics_withcontent_df_with_time.where(F.col("socialpublished_hours_from_anchor_point1") != F.col("socialpublished_hours_from_anchor_point")))

# COMMAND ----------

final_socialdata_withmetrics_withcontent_df_with_time = final_socialdata_withmetrics_withcontent_df_with_time.withColumn("brand", F.lit(brand_name))
final_socialdata_withmetrics_withcontent_df_with_time = final_socialdata_withmetrics_withcontent_df_with_time.withColumn("social_created_epoch_time", final_socialdata_withmetrics_withcontent_df_with_time["social_created_epoch_time"].cast(LongType()))

# COMMAND ----------

# final_socialdata_withmetrics_withcontent_df_with_time.printSchema()

# COMMAND ----------

# final_socialdata_withmetrics_withcontent_df_with_time.printSchema()

# COMMAND ----------

# change this as the coll hfacs became integer array

# COMMAND ----------

# final_socialdata_withmetrics_withcontent_df_with_time.printSchema()

# COMMAND ----------

final_socialdata_withmetrics_withcontent_df_with_time.write \
    .format("delta") \
    .partitionBy("brand") \
    .mode("overwrite") \
    .option("replaceWhere", "brand == '{}'".format(brand_name)) \
    .save("s3://cn-dse-falcon-{}/analysis/socialflow_social_{}_complete".format(mode, social_platform_name))

# COMMAND ----------

print("Final Socialdata Content written for brand {}!!!".format(brand_name))

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_socialflow_social_facebook_page_complete;
# CREATE TABLE falcon.analysis_socialflow_social_facebook_page_complete
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/socialflow_social_facebook_page_complete"

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_socialflow_social_twitter_complete;
# CREATE TABLE falcon.analysis_socialflow_social_twitter_complete
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/socialflow_social_twitter_complete"

# COMMAND ----------


