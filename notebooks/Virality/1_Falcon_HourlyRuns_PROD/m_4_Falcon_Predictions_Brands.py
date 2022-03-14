# Databricks notebook source
# dbutils.library.installPyPI("pyarrow", version='6.0.0')
# dbutils.library.restartPython()

# COMMAND ----------

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
#                                                 'them',
#                                                 'gq-germany'
#                                                ])
# dbutils.widgets.dropdown("social_platform_name", "facebook_page", ["facebook_page", "twitter"])
# dbutils.widgets.dropdown("social_partner_name", "socialflow", ["socialflow", "slack"])
# dbutils.widgets.dropdown("mode", "dev", ["dev", "prod"])
# dbutils.widgets.dropdown("data_for", "predict", ["predict", "train"])

# COMMAND ----------

# DBTITLE 1,Get the widget parameters
data_for = "predict"
brand_name = dbutils.widgets.get("brand_name")
mode = dbutils.widgets.get("mode")
social_platform_name = dbutils.widgets.get("social_platform_name")
social_partner_name = dbutils.widgets.get("social_partner_name")

if data_for == "predict":
  is_train = False
else:
  is_train = True

# COMMAND ----------

# DBTITLE 1,Imports
from falcon.utils.vault_utils import VaultAccess
from falcon.database.sqlalchemy_utils import SQLAlchemyUtils
# from falcon.utils.s3fs_utils import S3FSActions
from falcon.common import read_config, write_db_as_delta
from falcon.socialplatform_utils.socialflow_utils_kafka import Socialflow
from falcon.database.db_utils import DBUtils
from falcon.utils.datetime_utils import AnchorDateTimeUtils, DateTimeUtils
from falcon.inference.predict_model_kafka import processing_hold_df_forlabels, recycle_falcon_later_df_load_never_df
from falcon.scorer.prediction_data_preparation import All_Brand_Data_Preparation, Wired_Data_Preparation
from falcon.inference.brand_filters import *

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import udf
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

# DBTITLE 1,Database setup

vt_rds_data = VaultAccess(mode=mode).get_settings(settings_type="rds")
vt_socialplatform_data = VaultAccess(mode=mode).get_settings(settings_type=f"{social_partner_name}")

# COMMAND ----------

engine = SQLAlchemyUtils().get_engine(vt_rds_data)
metadata_common = MetaData(schema="common")

metadata_settings = MetaData(schema="settings")
metadata_brand = MetaData(schema="brand")

conn = engine.connect()

db_utils = DBUtils.init_without_db_config(conn, engine, metadata_common = metadata_common, metadata_settings = metadata_settings)

# COMMAND ----------

# DBTITLE 1,Read configs
content_config = read_config(settings_name="content")
traffic_config = read_config(settings_name="traffic")
socialcopy_config = read_config(settings_name="socialcopy_data")
falcon_config = read_config("falcon")


prediction_brand_config = db_utils.get_brand_config(brand_name, db_utils.get_social_platform_id(social_platform_name))

# COMMAND ----------

# DBTITLE 1,Get hfac
hfac = AnchorDateTimeUtils.get_currenthours_from_anchorpoint(
        traffic_config["anchor_date"]
    )


print(f"Current hour from anchorpoint is {hfac}")
logger.info(f"Current hour from anchorpoint is {hfac}")

# COMMAND ----------

# DBTITLE 1,Data Reader - Sparrow Data
from falcon.datareader.sparrow_reader import SparrowDataReader
sparrow_data_reader = SparrowDataReader(is_train=is_train, 
                                        brand_name=brand_name,
                                        hfac=hfac, 
                                        social_account_type=social_platform_name,
                                        mode=mode,
                                        brand_config = prediction_brand_config,
                                       spark=spark)
sparrow_df,_,_ = sparrow_data_reader.read_sparrow_data()
# sparrow_df.count()

# COMMAND ----------

# DBTITLE 1,Content Data Reader
from falcon.datareader.content_reader import ContentDataReader
content_data_reader = ContentDataReader(brand_config =prediction_brand_config,
                                        mode=mode,
                                       spark=spark)
content_df, identifiers_urls = content_data_reader.read_content_data()

# COMMAND ----------

# DBTITLE 1,SocialCopy Configs
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

# DBTITLE 1,SocialCopy Data Reader
from falcon.datareader.social_reader import SocialDataReader
socialcopy_data_reader = SocialDataReader(db_config, 
                                         is_train, 
                                         brand_name,
                                         social_platform_name,
                                         prediction_brand_config,
                                         s_obj,
                                         social_partner_name, 
                                         db_utils, 
                                         mode,
                                         spark)
socialcopy_df = socialcopy_data_reader.update_and_read_socialcopy_data()
# display(socialcopy_df)

# COMMAND ----------

final_content_df = content_data_reader.get_content_data_with_latest_socialcopy(socialcopy_df, content_df, identifiers_urls)

# COMMAND ----------

social_brand_content_df, _ = socialcopy_data_reader.get_socialposted_brand_data(identifiers_urls)

# COMMAND ----------

# DBTITLE 1,Featurizer Module
from falcon.featuregenerator.feature_generator import FeatureGenerator
feature_generator = FeatureGenerator(prediction_brand_config, 
                                     logger
                                    )
sparrow_content_final_df= feature_generator.sparrow_content_socialflow_data(sparrow_df, final_content_df, social_brand_content_df)
final_df = feature_generator.remove_published_before(sparrow_content_final_df)
checker = feature_generator.add_features_to_df(final_df)

# COMMAND ----------

# MAGIC %md ## Spark to Pandas and Running the prediction after data manipulation

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
prediction_df = checker.toPandas()

# COMMAND ----------

multiple_urls_vals_df = identifiers_urls.toPandas()
social_brand_content_df = social_brand_content_df.toPandas()
social_brand_content_df = social_brand_content_df.drop(["brand", "social_created_time_utc"], axis=1)

# COMMAND ----------

prediction_df

# COMMAND ----------

# DBTITLE 1,Final features for the model
prediction_df, feature_cols = feature_generator.prepare_logistic_features(prediction_df)

# COMMAND ----------

# DBTITLE 1,Code Check: Assumption is that the brand posts something in the last 2-3 weeks
import sys
if social_brand_content_df.shape[0] == 0:
  print("Make sure that some content was posted in the last 2-3 weeks, if not fail the dag and get back to it rather than pushing duplicates!!! Also failing because of non_social_posts being very high, check it out!!!")
  sys.exit(1)

# COMMAND ----------

# DBTITLE 1,Elements where copilot ids don't exist
social_brand_content_df.loc[social_brand_content_df["copilot_id"].isna(), :]

# COMMAND ----------

social_brand_content_df.loc[social_brand_content_df["copilot_id"] == "nan", :]

# COMMAND ----------

# MAGIC %md ## Filters Data Gathering

# COMMAND ----------

# MAGIC %md #### 1. Removing any cids/urls posted in the last x weeks(settings.yaml) for every brand. Social Posted data!

# COMMAND ----------

# Ignore the CIDs, URLS, and Cleaned URLs
ignore_urls = set(social_brand_content_df["url"].values)
ignore_cleaned_urls = set(social_brand_content_df["cleaned_url"].values)
ignore_copilot_ids = set(social_brand_content_df["copilot_id"].values)
ignore_urls = [url for url in ignore_urls if str(url) != "nan"]
ignore_cleaned_urls = [url for url in ignore_cleaned_urls if str(url) != "nan"]
ignore_copilot_ids = [cid for cid in ignore_copilot_ids if str(cid) != "nan"]

# COMMAND ----------

len(ignore_urls), len(ignore_cleaned_urls), len(ignore_copilot_ids)

# COMMAND ----------

# MAGIC %md #### 2. Get all the elements in the Hold, Schedule and Optimize queue

# COMMAND ----------

# s_obj.brand_forqueue_ids
(hold_brand_df, hold_nonbrand_df), (schedule_brand_df, schedule_nonbrand_df), (optimize_brand_df, optimize_nonbrand_df) = s_obj.get_socialflow_queues()

# COMMAND ----------

# DBTITLE 1,Section to debug if the queue elements are extracted correctly
# def display_queue_elements(df):
#   if df.shape[0] > 0:
#     display(df)

# display_queue_elements(hold_brand_df)
# display_queue_elements(hold_nonbrand_df)
# display_queue_elements(schedule_brand_df)
# display_queue_elements(schedule_nonbrand_df)
# display_queue_elements(optimize_brand_df)
# display_queue_elements(optimize_nonbrand_df)

# COMMAND ----------

# DBTITLE 1,Section to debug if the total counts of elements returned from queues is correct (add to hourly model runs dashboard)
print("CHECK IF THIS IS WHERE THE ERROR IS!!!")
print(hold_brand_df.shape[0] + schedule_brand_df.shape[0] + optimize_brand_df.shape[0])

# COMMAND ----------

# MAGIC %md #### 3. Get all the elements from Falcon Later and Falcon Never databases

# COMMAND ----------

# DBTITLE 1,Databases updated for falcon_never and falcon_later
## Need to check this query properly if the elements are getting deleted or not

kwargs = {
    "s_obj": s_obj,
    "brand_id" : db_utils.get_brand_id(brand_name),
    "db_config": db_config,
    "social_platform_id": db_utils.get_social_platform_id(social_platform_name),
    "mode": mode
}

logger.info(f"Hold DF shape before processing for labels {hold_brand_df.shape}")
hold_brand_df, hold_nonbrand_df = processing_hold_df_forlabels(hold_brand_df, **kwargs)
logger.info(f"Hold DF shape after processing for labels {hold_brand_df.shape}")

print("**"*50)
(falcon_later_brand_df, falcon_later_nonbrand_df), (
        falcon_never_brand_df,
        falcon_never_nonbrand_df,
    ) = recycle_falcon_later_df_load_never_df(**kwargs)

# COMMAND ----------

# MAGIC %md #### 4. Remove any duplicate elements in the Hold queue(should not happen if there are no errors, sanity check at every run)

# COMMAND ----------

# DBTITLE 1,All Brand Data Preparation class
pred_data_preparation = All_Brand_Data_Preparation(s_obj)

# COMMAND ----------

# DBTITLE 1,Check duplicate entries in the hold queue and delete all the earliest duplicate copies of the posts
if hold_brand_df.shape[0] > 0:
  print("There are duplicated being added in the run, check out whats the reason, check the URLs and if they are giving Exceptions!!!")
  hold_brand_df, duplicated_hold_df = pred_data_preparation.remove_duplicates_hold_queue(hold_brand_df)
  print(f"Duplicated hold df shape:: {duplicated_hold_df.shape[0]}")
else:
  print("Hold Dataframes are empty... tell the brands, a redash element!!!")

# COMMAND ----------

# MAGIC %md #### 5. For Wired, we keep x Gear, y Science and z Backchannel recs always in the hold queue. Can be moved to brand_filters.py in the falcon library.

# COMMAND ----------

# **********************
# TODO: Change for WIRED (Move at a brand specific place)

if brand_name == "wired":
  wired_data_preparation= Wired_Data_Preparation()

  (
      remaining_wired_gear_value,
      remaining_wired_science_value,
      remaining_wired_backchannel_value,
  ) = wired_data_preparation.prepare_custom_data(hold_brand_df=hold_brand_df)
      
      
  logger.info("Running for BRAND WIRED !!!")
  logger.info(
      f"Remaining for BRAND WIRED Gear Values {remaining_wired_gear_value}!!!"
  )
  logger.info(
      f"Remaining for BRAND WIRED Science Values {remaining_wired_science_value}!!!"
  )
  logger.info(
      f"Remaining for BRAND WIRED Backchannel Values {remaining_wired_backchannel_value}!!!"
  )

# **********************

# COMMAND ----------

# MAGIC %md ### Preparing Prediction Dataset before Filters are applied and debugging to see if everything is running fine (can add logging statements to dashboard)

# COMMAND ----------

logger.info(f"HOLD BRAND QUEUE {hold_brand_df.shape}, NON BRAND QUEUE {hold_nonbrand_df.shape}")
logger.info(f"SCHEDULE BRAND QUEUE {schedule_brand_df.shape}, NON BRAND QUEUE {schedule_nonbrand_df.shape}")
logger.info(f"OPTIMIZE BRAND QUEUE {optimize_brand_df.shape}, NON BRAND QUEUE {optimize_nonbrand_df.shape}")
logger.info(f"FALCON LATER BRAND QUEUE {falcon_later_brand_df.shape}, NON BRAND QUEUE {falcon_later_nonbrand_df.shape}")
logger.info(f"FALCON NEVER BRAND QUEUE {falcon_never_brand_df.shape}, NON BRAND QUEUE {falcon_never_nonbrand_df.shape}")

# COMMAND ----------

hold_brand_df = pred_data_preparation.add_copilot_ids_to_brand_dfs(hold_brand_df, multiple_urls_vals_df)
schedule_brand_df = pred_data_preparation.add_copilot_ids_to_brand_dfs(schedule_brand_df, multiple_urls_vals_df)
optimize_brand_df = pred_data_preparation.add_copilot_ids_to_brand_dfs(optimize_brand_df, multiple_urls_vals_df)
falcon_later_brand_df = pred_data_preparation.add_copilot_ids_to_brand_dfs(falcon_later_brand_df, multiple_urls_vals_df)
falcon_never_brand_df = pred_data_preparation.add_copilot_ids_to_brand_dfs(falcon_never_brand_df, multiple_urls_vals_df)

# COMMAND ----------

## Add boolean columns even if the dataframes for respective queues are empty
df_predictions1 = pred_data_preparation.add_descriptive_boolean_cols(
    prediction_df, hold_brand_df, added_colname="is_hold")

## Add boolean columns even if the dataframes for respective queues are empty
df_predictions1 = pred_data_preparation.add_descriptive_boolean_cols(
  df_predictions1, schedule_brand_df, added_colname="is_scheduled"
)

## Add boolean columns even if the dataframes for respective queues are empty
df_predictions1 = pred_data_preparation.add_descriptive_boolean_cols(
  df_predictions1, optimize_brand_df, added_colname="is_optimized"
)

## Add boolean columns even if the dataframes for respective queues are empty
df_predictions1 = pred_data_preparation.add_descriptive_boolean_cols(
    df_predictions1, falcon_never_brand_df, added_colname="is_falcon_never"
)

## Add boolean columns even if the dataframes for respective queues are empty
df_predictions1 = pred_data_preparation.add_descriptive_boolean_cols(
    df_predictions1, falcon_later_brand_df, added_colname="is_falcon_later"
)
# df_predictions1.loc[df_predictions1.is_hold == True, :].shape
# df_predictions1.loc[df_predictions1.is_scheduled == True, :].shape
# df_predictions1.loc[df_predictions1.is_optimized == True, :].shape
# df_predictions1.loc[df_predictions1.is_falcon_never == True, :].shape
# df_predictions1.loc[df_predictions1.is_falcon_later == True, :].shape

# COMMAND ----------

# DBTITLE 1,Current Hour Metadata before predictions
# Finally making Falcon Predictions
anchor_date_str = traffic_config["anchor_date"]
tz = pendulum.timezone("America/New_York")
prediction_hour = AnchorDateTimeUtils.get_currenthours_from_anchorpoint(
    anchor_date_str
)
anchor_dt = pendulum.parse(anchor_date_str)
logger.info(f"Prediction Hour is {prediction_hour}")
prediction_time_utc = anchor_dt.add(hours=prediction_hour)
logger.info(f"Prediction time in UTC timezone {prediction_time_utc}")
prediction_time_est = tz.convert(prediction_time_utc)
logger.info(f"Prediction time in EST timezone {prediction_time_est}")

logger.info(f"Falcon prediction dataframe shape: {df_predictions1.shape}")

# COMMAND ----------

prediction_traffic_raw_df, recommendation_traffic_raw_df = df_predictions1.copy(), df_predictions1.copy()

# COMMAND ----------

# DBTITLE 1,Verify if the shapes are correct, or if data frames are empty
prediction_traffic_raw_df.shape, recommendation_traffic_raw_df.shape

# COMMAND ----------

# MAGIC %md ## Filter Application by ignore copilots preared earlier

# COMMAND ----------

# Filter 1: Final Prediction Dataframe, Remove the content that has been posted on FB

prediction_traffic_df = pred_data_preparation.filter_copilots_from_prediciton_data(prediction_traffic_raw_df,ignore_copilot_ids)


# Filter 2: Ignoring the copilot ids which are already in falcon later or falcon never dataframe
falcon_ignore_cids = set(list(falcon_later_brand_df.copilot_id.values if falcon_later_brand_df.shape[0] > 0 else []) + list(falcon_never_brand_df.copilot_id.values if falcon_never_brand_df.shape[0] > 0 else []))
prediction_traffic_df = pred_data_preparation.filter_copilots_from_prediciton_data(prediction_traffic_df, falcon_ignore_cids)

# COMMAND ----------

#### RECS CHANGE: If its used as online recs without applying the helps, helps catch issues with missing data
recommendation_traffic_df = pred_data_preparation.filter_copilots_from_prediciton_data(recommendation_traffic_raw_df,falcon_ignore_cids)

# COMMAND ----------

# DBTITLE 1,Filter by socialflow ignore ids
# Filter 3: Ignoring the copilot ids which are already on any of the socialflow queues
socialflow_ignore_cids = set(
    list(hold_brand_df.copilot_id.values if hold_brand_df.shape[0] > 0 else [])
    + list(schedule_brand_df.copilot_id.values if schedule_brand_df.shape[0] > 0 else [])
    + list(optimize_brand_df.copilot_id.values if optimize_brand_df.shape[0] > 0 else [])
)
prediction_traffic_df = pred_data_preparation.filter_copilots_from_prediciton_data(prediction_traffic_df, socialflow_ignore_cids)

# COMMAND ----------

# DBTITLE 1,Select content based on published data/year
# Filter 4
# Filter on min pub date epoch time to only consider articles after content min pub date,
# added as a check later if the brand changes the value after going live

prediction_traffic_df = pred_data_preparation.filter_articles_by_published(prediction_traffic_df, prediction_brand_config)

# COMMAND ----------

#### RECS CHANGE: Helps figure out if any content has been posted by the brand in the last 25 days
# Filter on min pub date epoch time to only consider articles after content min pub date, added as a check later if the brand changes the value after going live
if brand_name == "wired":
  recs_minpubdate_epochtime = DateTimeUtils.convert_datetimestr_epochtime(DateTimeUtils.get_now_datetime().subtract(days=25).to_date_string())
else:
  recs_minpubdate_epochtime = DateTimeUtils.convert_datetimestr_epochtime(DateTimeUtils.get_now_datetime().subtract(days=25).to_date_string())

recommendation_traffic_df = recommendation_traffic_df.loc[recommendation_traffic_df["pub_date_epoch"] >= int(recs_minpubdate_epochtime), :]
recommendation_traffic_df = recommendation_traffic_df.reset_index(drop=True)

logger.info(
    f"Recommendation traffic dataframe shape for Pub Date in last days for recommendation: {recommendation_traffic_df.shape}"
)

# COMMAND ----------

# DBTITLE 1,If this gives an error, no content has been written/updated by the brand in the last 25 days, check the streams!!!
if recommendation_traffic_df.shape[0] == 0:
  print("Recommendation Traffic DF should have some content as there needs to be some content trending in the last 25 days!!!")
  sys.exit(1)
else:
  print(recommendation_traffic_df.shape[0])

# COMMAND ----------

assert prediction_traffic_df.is_hold.sum() == 0
assert prediction_traffic_df.is_scheduled.sum() == 0
assert prediction_traffic_df.is_optimized.sum() == 0
assert prediction_traffic_df.is_falcon_later.sum() == 0
assert prediction_traffic_df.is_falcon_never.sum() == 0

# COMMAND ----------

# DBTITLE 1,Custom brand filters
# Filter 6: Brand Specific Filters
prediction_tags_df = prediction_traffic_df.explode("tags").loc[:, ["cid", "tags"]].drop_duplicates().dropna()
##### BRAND FILTERS
brand_kwargs = {
  
  "brand_name": brand_name,
  "brand_config": prediction_brand_config,
  "mode": mode,
  "hold_df": hold_brand_df,
  "multiple_urls_vals_df": multiple_urls_vals_df,
  "prediction_tags_df": prediction_tags_df,
  "platform_name": social_platform_name
}

brand_filters_constructor = globals()[
    "{}_Filters_Exclusions".format(brand_name.replace("-", "").upper())
]


brand_filters_instance = brand_filters_constructor(
    prediction_traffic_df, **brand_kwargs
)

logger.info(f"BEFORE BRAND FILTERS AND EXCLUSIONS : {prediction_traffic_df.shape}")
brand_filters_instance.brand_filters()
brand_filters_instance.brand_exclusions()
prediction_traffic_df = brand_filters_instance.prediction_traffic_df
logger.info(f"AFTER BRAND FILTERS AND EXCLUSIONS : {prediction_traffic_df.shape}")
logger.info("**" * 50)
# **********

# COMMAND ----------

# MAGIC %md ## MODEL PREDICTION

# COMMAND ----------

# Running the model
# if mode is dev with custom version pass model_version
from falcon.scorer.prediction import Scorer
scorer= Scorer(brand_name, social_platform_name, mode)
features = [
    "total_pageviews_prev_3hrs_2hrs",
    "total_pageviews_prev_2hrs_1hrs",
    "total_pageviews_prev_1hrs",
    "social_visits_prev_3hrs_2hrs",
    "social_visits_prev_2hrs_1hrs",
    "social_visits_prev_1hrs",
    "content_type",
    "content_recency_when_accessed"
]

outcomes = scorer.predict(prediction_traffic_df, features)
total_outcomes = outcomes.copy()

# COMMAND ----------

outcomes.content_recency_when_accessed.unique()

# COMMAND ----------

min_article_recipes = prediction_brand_config["falcon"]["output_content_type_min_counts"]["story_recipes"]
min_gallery_reviews = prediction_brand_config["falcon"]["output_content_type_min_counts"]["gallery_reviews"]

# COMMAND ----------

# **********************
# Wired changes for limiting science, gear and backchannel stories
from falcon.scorer.recommendation import *
if brand_name == "wired":
  
  brand_recs_constructor = globals()[
    "{}_RECOMMENDATIONS".format(brand_name.replace("-", "").upper())
  ]

  brand_recs_instance = brand_recs_constructor(
      outcomes, min_article_recipes, min_gallery_reviews
  )
  brand_recs_kwargs = {
    "remaining_wired_gear_value":remaining_wired_gear_value,
    "remaining_wired_science_value" : remaining_wired_science_value,
    "remaining_wired_backchannel_value" : remaining_wired_backchannel_value,
    "prediction_hour" :  prediction_hour

  }
  outcomes_socialflow = brand_recs_instance.get_recommendations(**brand_recs_kwargs)

else:

  brand_recs_constructor = globals()[
    "BRAND_RECOMMENDATIONS".upper()
  ]
  
  brand_recs_instance = brand_recs_constructor(
      outcomes, min_article_recipes, min_gallery_reviews
  )
  brand_recs_kwargs = {
    "prediction_hour" :  prediction_hour
  }
  
  outcomes_socialflow = brand_recs_instance.get_recommendations(**brand_recs_kwargs)  

# COMMAND ----------


outcomes_socialflow_identifiers = outcomes_socialflow.drop(["copilot_id"], axis=1)

# COMMAND ----------

# DBTITLE 1,Inner join with identifiers
outcomes_socialflow_identifiers = pd.merge(outcomes_socialflow_identifiers, multiple_urls_vals_df, how="inner", left_on="cid", right_on="copilot_id")
# outcomes_socialflow_identifiers

# COMMAND ----------

# DBTITLE 1,Drop duplicates from the recommendations based on cid
outcomes_socialflow_identifiers = outcomes_socialflow_identifiers.drop_duplicates(
    keep="first", subset=["cid"]
)

# COMMAND ----------

outcomes_socialflow_identifiers[['cid','copilot_id_urls']]

# COMMAND ----------

# MAGIC %md ## Post on Socialflow

# COMMAND ----------

from falcon.scorer.publish_to_socialflow import PublishToSocialflow

pub_to_sf_obj = PublishToSocialflow(s_obj, falcon_config, outcomes_socialflow_identifiers)

pub_to_sf_obj.publish_outcomes_to_socialflow()

# COMMAND ----------

# pd.set_option('display.max_colwidth', 300)
# outcomes_socialflow_identifiers.loc[:, ["cid", "long_url"]]

# COMMAND ----------

# outcomes_socialflow_identifiers

# COMMAND ----------

# MAGIC %md ## Update the databases

# COMMAND ----------

outcomes_socialflow_identifiers['brand_id'] = db_utils.get_brand_id(brand_name)
outcomes_socialflow_identifiers['social_platform_id'] = db_utils.get_social_platform_id(social_platform_name)
outcomes_socialflow_identifiers['model_name'] = f"falcon_{brand_name}_{social_platform_name}_logistic_prod"


total_outcomes['brand_id'] = db_utils.get_brand_id(brand_name)
total_outcomes['social_platform_id'] = db_utils.get_social_platform_id(social_platform_name)
total_outcomes['model_name'] = f"falcon_{brand_name}_{social_platform_name}_logistic_prod"

# COMMAND ----------

outcomes_socialflow_identifiers = outcomes_socialflow_identifiers.loc[:, ['brand_id',
       'social_platform_id', 'hours_from_anchor_point','cid','total_hourly_events_pageviews',
       'anon_visits', 'direct_visits', 'referral_visits', 'internal_visits',
       'search_visits', 'search_yahoo_visits', 'search_bing_visits',
       'search_google_visits', 'social_visits', 'social_facebook_visits',
       'social_instagram_visits', 'social_twitter_visits', 'final_social_posted',
       'total_pageviews_prev_3hrs_2hrs', 'total_pageviews_prev_2hrs_1hrs',
       'total_pageviews_prev_1hrs', 'total_pageviews_next_6hrs',
       'anon_visits_prev_3hrs_2hrs', 'anon_visits_prev_2hrs_1hrs',
       'anon_visits_prev_1hrs', 'anon_visits_next_6hrs',
       'social_visits_prev_3hrs_2hrs', 'social_visits_prev_2hrs_1hrs',
       'social_visits_prev_1hrs', 'social_visits_next_6hrs',
       'social_facebook_visits_prev_3hrs_2hrs',
       'social_facebook_visits_prev_2hrs_1hrs',
       'social_facebook_visits_prev_1hrs', 'social_facebook_visits_next_6hrs',
       'total_pageviews', 'total_anon_visits_prev_1hrs_3hrs',
       'average_hourly_events', 'brand',
       'content_type', 'revision', 'headline', 'dek',
       'contentsource', 'channel', 'subchannel', 'seo_title',
       'seo_description', 'socialtitle', 'socialdescription', 'pub_date_epoch', 'original_pubdate_epoch',
       'socialcopy', 'social_created_epoch_time', 'pubdate_diff_anchor_point', 'content_recency_when_accessed', 'content_type_description', 'is_hold',
       'is_scheduled', 'is_optimized', 'is_falcon_never', 'is_falcon_later', 'model_name',
       'model_score']]

outcomes_socialflow_identifiers_db = outcomes_socialflow_identifiers.loc[:, ['brand_id', 'social_platform_id', 'hours_from_anchor_point', 'cid', 'content_type', 'model_name', 'model_score']]

# COMMAND ----------

total_outcomes = total_outcomes.loc[:, ['brand_id', 'social_platform_id', 'hours_from_anchor_point', 'cid', 'total_hourly_events_pageviews',
       'anon_visits', 'direct_visits', 'referral_visits', 'internal_visits',
       'search_visits', 'search_yahoo_visits', 'search_bing_visits',
       'search_google_visits', 'social_visits', 'social_facebook_visits',
       'social_instagram_visits', 'social_twitter_visits', 'final_social_posted',
       'total_pageviews_prev_3hrs_2hrs', 'total_pageviews_prev_2hrs_1hrs',
       'total_pageviews_prev_1hrs', 'total_pageviews_next_6hrs',
       'anon_visits_prev_3hrs_2hrs', 'anon_visits_prev_2hrs_1hrs',
       'anon_visits_prev_1hrs', 'anon_visits_next_6hrs',
       'social_visits_prev_3hrs_2hrs', 'social_visits_prev_2hrs_1hrs',
       'social_visits_prev_1hrs', 'social_visits_next_6hrs',
       'social_facebook_visits_prev_3hrs_2hrs',
       'social_facebook_visits_prev_2hrs_1hrs',
       'social_facebook_visits_prev_1hrs', 'social_facebook_visits_next_6hrs',
       'total_pageviews', 'total_anon_visits_prev_1hrs_3hrs',
       'average_hourly_events', 'brand',
       'content_type', 'revision', 'headline', 'dek',
       'contentsource', 'channel', 'subchannel', 'seo_title',
       'seo_description', 'socialtitle', 'socialdescription', 'pub_date_epoch', 'original_pubdate_epoch',
       'socialcopy', 'social_created_epoch_time', 'pubdate_diff_anchor_point', 'content_recency_when_accessed', 'content_type_description', 'is_hold',
       'is_scheduled', 'is_optimized', 'is_falcon_never', 'is_falcon_later',
       'model_name', 'model_score']]

total_outcomes_db = total_outcomes.loc[:, ['brand_id', 'social_platform_id', 'hours_from_anchor_point', 'cid', 'content_type', 'model_name', 'model_score']]

# COMMAND ----------

# Inserting socialflow outcomes in the db table
assert outcomes_socialflow_identifiers_db.shape[0] > 0
# outcomes_socialflow_identifiers["socialcopy"] = outcomes_socialflow_identifiers.socialcopy.fillna("")
outcomes_socialflow_identifiers_db = outcomes_socialflow_identifiers_db.reset_index(drop=True)

# COMMAND ----------

from sqlalchemy import text, and_
db_utils = DBUtils.init_with_db_config(db_config)

predicate = and_(text("brand_id='{}'".format(db_utils.get_brand_id(brand_name))),
                      text("social_platform_id='{}'".format(db_utils.get_social_platform_id(social_platform_name))))
print(predicate)
result_proxy = db_utils.delete_from_table(socialflow_queue_posted_data_db_table, predicate=predicate)

# COMMAND ----------

logger.info(
    f"Deleting from Socialflow queue before new inserts {result_proxy.rowcount}"
)

rowcount = db_utils.insert_element_to_table(socialflow_queue_posted_data_db_table, outcomes_socialflow_identifiers_db.to_dict(orient="records"))
# with db_connection.begin() as trans:
#     result_proxy1 = db_connection.execute(
#         socialflow_queue_posted_data_db_table.insert(),
#         outcomes_socialflow_identifiers.to_dict(orient="records"),
#     )
logger.info(f"Inserts into socialflow queue {rowcount}")

# COMMAND ----------

# Inserting model outcomes in the db table
#total_outcomes["socialcopy"] = total_outcomes.socialcopy.fillna("")
total_outcomes_db = total_outcomes_db.reset_index(drop=True)

result_proxy1 = db_utils.delete_from_table(model_outcomes_db_table, predicate=predicate)
# with db_connection.begin() as trans:
#     stmt1 = delete(model_outcomes_db_table)
#     result_proxy1 = db_connection.execute(stmt1)
logger.info(
    f"Deleting from Model Outcomes Table before new inserts {result_proxy1.rowcount}"
)

rowcount1 = db_utils.insert_element_to_table(model_outcomes_db_table, total_outcomes_db.to_dict(orient="records"))
# with db_connection.begin() as trans:
#     result_proxy1 = db_connection.execute(
#         model_outcomes_db_table.insert(), total_outcomes.to_dict(orient="records")
#     )
logger.info(
    f"New recommendations inserted in Model Outcomes {rowcount1}"
)

# COMMAND ----------

# DBTITLE 1,Write Falcon Later to S3 locations



if "cleaned_url" in falcon_later_brand_df.columns:
  falcon_later_brand_df = falcon_later_brand_df.drop(["cleaned_url"], axis=1)
if "copilot_id" in falcon_later_brand_df.columns:
  falcon_later_brand_df = falcon_later_brand_df.drop(["copilot_id"], axis=1) 

falcon_later_path = falcon_config["later_data"]["s3_bucket"][mode]
write_db_as_delta(falcon_later_brand_df, brand_name, social_platform_name, falcon_later_path, hfac, spark=spark)

# COMMAND ----------

if "cleaned_url" in falcon_never_brand_df.columns:
  falcon_never_brand_df = falcon_never_brand_df.drop(["cleaned_url"], axis=1)
if "copilot_id" in falcon_never_brand_df.columns:
  falcon_never_brand_df = falcon_never_brand_df.drop(["copilot_id"], axis=1) 

falcon_never_path = falcon_config["never_data"]["s3_bucket"][mode]
write_db_as_delta(falcon_never_brand_df, brand_name, social_platform_name, falcon_never_path, hfac, spark=spark)

# COMMAND ----------

# DBTITLE 1,Write Outcome Data to S3 Locations

spark.conf.set("spark.sql.parquet.mergeSchema", "true")
falcon_outcome_path = falcon_config["outcome_data"]["s3_bucket"][mode]
tot_spark_df, tot_spark_df_schema = write_db_as_delta(total_outcomes, brand_name, social_platform_name, falcon_outcome_path, hfac, spark=spark)
tot_spark_df_schema

# COMMAND ----------

# DBTITLE 1,Write Social Posted Data to S3 Locations


falcon_socialflow_queue_path = falcon_config["socialflow_queue_posted_data"]["s3_bucket"][mode]
sqpd_df, sqpd_df_schema = write_db_as_delta(outcomes_socialflow_identifiers, brand_name, social_platform_name, falcon_socialflow_queue_path, hfac, tot_spark_df_schema, spark=spark)

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


