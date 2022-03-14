# Databricks notebook source
# dbutils.widgets.dropdown("data_for", "predict", ["predict", "train"])
# dbutils.widgets.dropdown("mode", "dev", ["dev", "prod"])

# COMMAND ----------

# DBTITLE 1,Create sparrow data for training or prediction
data_for = dbutils.widgets.get("data_for")
mode = dbutils.widgets.get("mode")
if data_for == "predict":
  is_train = False
else:
  is_train = True

# COMMAND ----------

# DBTITLE 1,Imports
from falcon.utils.vault_utils import VaultAccess
from falcon.database.sqlalchemy_utils import SQLAlchemyUtils
# from falcon.utils.s3fs_utils import S3FSActions
from falcon.common import get_unique_brandnames_source, read_config
from falcon.database.db_utils import DBUtils
from sqlalchemy import MetaData
from falcon.utils.datetime_utils import AnchorDateTimeUtils
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pendulum
from pyspark.sql.types import *
from pyspark.sql.functions import UserDefinedFunction
import os

import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# COMMAND ----------

mode = dbutils.widgets.get("mode")

vt_rds_data = VaultAccess(mode=mode).get_settings(settings_type="rds")


## Databases Used

engine = SQLAlchemyUtils().get_engine(vt_rds_data)
metadata_common = MetaData(schema="common")

metadata_settings = MetaData(schema="settings")
conn = engine.connect()

db_utils = DBUtils.init_without_db_config(conn, engine, metadata_common = metadata_common, metadata_settings = metadata_settings)


brand_config = db_utils.get_all_brand_config()
traffic_config = read_config(settings_name="traffic")
falcon_config = read_config("falcon")

# COMMAND ----------

hfac = AnchorDateTimeUtils.get_currenthours_from_anchorpoint(
        traffic_config["anchor_date"]
    )
print(f"Current hour is {hfac}")

# COMMAND ----------

# DBTITLE 1,Read Sparrow data
class DataFrames:
    
  def __init__(self, is_train):  
    
    self.traffic_data_pq_df = self.read_sparrow_df(is_train)
    
  #########################
  ## TRAFFIC: SPARROW DF ##
  #########################
  
  def read_sparrow_df(self, is_train):
    """
    Data is partitioned on Brand Name and Hours from anchor point
    """
    logger.info("\nStarted Reading Sparrow Data...")
    
    try:      

      sparrow_brand_names = get_unique_brandnames_source(brand_config,'sparrow')
      print(sparrow_brand_names)
      if is_train:
        get_definedhours_from_anchorpoint = UserDefinedFunction(AnchorDateTimeUtils.get_definedhours_from_anchorpoint, IntegerType())
        print("Doing it for Training Data")
        traindata_start_date = pendulum.datetime(2021, 1, 1, tz="UTC")
        traindata_end_date = pendulum.datetime(2021, 7, 1, tz="UTC")
        end_hour = AnchorDateTimeUtils().get_definedhours_from_anchorpoint(traffic_config["anchor_date"], traindata_end_date.to_date_string())
        start_hour = AnchorDateTimeUtils().get_definedhours_from_anchorpoint(traffic_config["anchor_date"], traindata_start_date.to_date_string())
        input_df = spark.table("sparrow_prod.sparrow").where(F.col("dt") >= traindata_start_date).where(F.col("dt") < traindata_end_date).where(F.lower("_t") == "pageview")\
                                .where(F.lower("_o").isin(sparrow_brand_names))\
                                .select(["_o", "_ts", "cid", "xid", "prt", "prs"])\
                                .withColumn("timestamp_utc", F.col("_ts").cast(TimestampType()))\
                                .withColumn("hours_from_anchor_point", get_definedhours_from_anchorpoint(F.lit(traffic_config["anchor_date"]), F.lit(F.col("_ts")))).drop("_ts")\
                                .withColumnRenamed("_o", "brand")
      else:
        print("Doing it for Prediction Data")
        print("Reading from {}".format(traffic_config["sparrow"]["data"]["s3_bucket"][mode]))
        sparrow_s3_bucket = traffic_config["sparrow"]["data"]["s3_bucket"][mode] # fix it to prod mode for dev also as the data is written in prod mode only, for development fix the location
        end_hour = AnchorDateTimeUtils.get_currenthours_from_anchorpoint(traffic_config["anchor_date"])
        start_hour = end_hour - traffic_config["default_hours"]
        input_df = spark.read.format("delta").load(sparrow_s3_bucket).where(F.col("brand").isin(sparrow_brand_names))
        input_df = input_df.where(F.col("hours_from_anchor_point") < end_hour).where(F.col("hours_from_anchor_point") >= start_hour)
        if not input_df:
          raise ValueError("Data does not exist for Sparrow for " + logdate)
        logger.info("Finished Reading Sparrow Data!!!\n")
      print(start_hour, end_hour)
      return input_df
    except Exception as e:
      logger.error("Data processing error!!!", exc_info=True)
      return None

# COMMAND ----------

traffic_config["default_hours"]

# COMMAND ----------

dfs = DataFrames(is_train)
dfs.traffic_data_pq_df.cache()
# dfs.traffic_data_pq_df.count()

# COMMAND ----------

# MAGIC %md ## Sparrow Feature Creation

# COMMAND ----------

grouped_brand_df = dfs.traffic_data_pq_df.groupby(["brand", "hours_from_anchor_point", "cid"])\
                        .agg(F.count("*").alias("total_hourly_events_pageviews"),\
                             F.sum(F.when(F.isnan("xid") | F.col("xid").isNull(), 1).otherwise(0)).alias("anon_visits"),\
                             F.sum(F.when(F.col("prt") == "direct", 1).otherwise(0)).alias("direct_visits"),\
                             F.sum(F.when(F.col("prt") == "referral", 1).otherwise(0)).alias("referral_visits"),\
                             F.sum(F.when(F.col("prt") == "internal", 1).otherwise(0)).alias("internal_visits"),\
                             F.sum(F.when(((F.col("prt") == "search")), 1).otherwise(0)).alias("search_visits"),\
                             F.sum(F.when(((F.col("prt") == "search") & (F.col("prs") == "yahoo")), 1).otherwise(0)).alias("search_yahoo_visits"),\
                             F.sum(F.when(((F.col("prt") == "search") & (F.col("prs") == "bing")), 1).otherwise(0)).alias("search_bing_visits"),\
                             F.sum(F.when(((F.col("prt") == "search") & (F.col("prs") == "google")), 1).otherwise(0)).alias("search_google_visits"),\
                             F.sum(F.when(((F.col("prt") == "social")), 1).otherwise(0)).alias("social_visits"),\
                             F.sum(F.when(((F.col("prt") == "social") & (F.col("prs") == "facebook")), 1).otherwise(0)).alias("social_facebook_visits"),\
                             F.sum(F.when(((F.col("prt") == "social") & (F.col("prs") == "instagram")), 1).otherwise(0)).alias("social_instagram_visits"),\
                             F.sum(F.when(((F.col("prt") == "social") & (F.col("prs") == "twitter")), 1).otherwise(0)).alias("social_twitter_visits"),\
                            )

# COMMAND ----------

window_prev_3hrs_2hrs = Window.partitionBy("brand", "cid").orderBy("hours_from_anchor_point").rangeBetween(Window.currentRow-3, Window.currentRow-2)
window_prev_2hrs_1hrs = Window.partitionBy("brand", "cid").orderBy("hours_from_anchor_point").rangeBetween(Window.currentRow-2, Window.currentRow-1)
window_prev_1hrs_curr = Window.partitionBy("brand", "cid").orderBy("hours_from_anchor_point").rangeBetween(Window.currentRow-1, Window.currentRow)
window_curr_next_6hrs = Window.partitionBy("brand", "cid").orderBy("hours_from_anchor_point").rangeBetween(Window.currentRow, Window.currentRow+6)

window_till_now = Window.partitionBy("brand", "cid").orderBy("hours_from_anchor_point").rangeBetween(Window.unboundedPreceding, Window.currentRow)
window_prev_3hrs_1hrs = Window.partitionBy("brand", "cid").orderBy("hours_from_anchor_point").rangeBetween(Window.currentRow-3, Window.currentRow-1)
window_prev_2hrs_curr = Window.partitionBy("brand", "cid").orderBy("hours_from_anchor_point").rangeBetween(Window.currentRow-2, Window.currentRow)

rolling_average_df = grouped_brand_df.select("*",

                  # Total Hourly Events Window Variables
                  F.sum("total_hourly_events_pageviews").over(window_prev_3hrs_2hrs).alias("total_pageviews_prev_3hrs_2hrs"),
                  F.sum("total_hourly_events_pageviews").over(window_prev_2hrs_1hrs).alias("total_pageviews_prev_2hrs_1hrs"),
                  F.sum("total_hourly_events_pageviews").over(window_prev_1hrs_curr).alias("total_pageviews_prev_1hrs"),
                  F.sum("total_hourly_events_pageviews").over(window_curr_next_6hrs).alias("total_pageviews_next_6hrs"),

                  # Anonymous Window Variables
                  F.sum("anon_visits").over(window_prev_3hrs_2hrs).alias("anon_visits_prev_3hrs_2hrs"),
                  F.sum("anon_visits").over(window_prev_2hrs_1hrs).alias("anon_visits_prev_2hrs_1hrs"),
                  F.sum("anon_visits").over(window_prev_1hrs_curr).alias("anon_visits_prev_1hrs"),
                  F.sum("anon_visits").over(window_curr_next_6hrs).alias("anon_visits_next_6hrs"),

                  # Social Window Variables
                  F.sum("social_visits").over(window_prev_3hrs_2hrs).alias("social_visits_prev_3hrs_2hrs"),
                  F.sum("social_visits").over(window_prev_2hrs_1hrs).alias("social_visits_prev_2hrs_1hrs"),
                  F.sum("social_visits").over(window_prev_1hrs_curr).alias("social_visits_prev_1hrs"),
                  F.sum("social_visits").over(window_curr_next_6hrs).alias("social_visits_next_6hrs"),

                  # Facebook Window Variables
                  F.sum("social_facebook_visits").over(window_prev_3hrs_2hrs).alias("social_facebook_visits_prev_3hrs_2hrs"),
                  F.sum("social_facebook_visits").over(window_prev_2hrs_1hrs).alias("social_facebook_visits_prev_2hrs_1hrs"),
                  F.sum("social_facebook_visits").over(window_prev_1hrs_curr).alias("social_facebook_visits_prev_1hrs"),
                  F.sum("social_facebook_visits").over(window_curr_next_6hrs).alias("social_facebook_visits_next_6hrs"),

                  # Total Pageviews till now
                  F.sum("total_hourly_events_pageviews").over(window_till_now).alias("total_pageviews"),

                  # Total Anonymous Visits Previous 1 hours - 3 hours
                  F.sum("anon_visits").over(window_prev_3hrs_1hrs).alias("total_anon_visits_prev_1hrs_3hrs"),

                  # Total Anonymous Visits Previous 1 hours - 3 hours
                  F.avg("total_hourly_events_pageviews").over(window_prev_2hrs_curr).alias("average_hourly_events"),
)

# COMMAND ----------

final_output_df = rolling_average_df.where(F.length("cid") > 1) # to eliminate cids where it might result in some null characters (do check the results once to see more exceptions)

# COMMAND ----------

final_output_df = final_output_df.na.fill(0)

# COMMAND ----------

# DBTITLE 1,Write the data to s3 locations
if data_for == "train":
  write_data_path = os.path.join(falcon_config["train_data"]["s3_bucket"][mode], "latest")
  print(write_data_path)
  final_output_df.write.partitionBy("brand").format("delta").mode("overwrite").save(write_data_path)
else:
  write_data_path = falcon_config["test_data"]["s3_bucket"][mode]
  print(write_data_path)
  final_output_df.withColumn("hfac", F.lit(hfac)).write.partitionBy("brand", "hfac").format("delta").mode("overwrite").save(write_data_path)

# COMMAND ----------

# final_output_df.count()

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


