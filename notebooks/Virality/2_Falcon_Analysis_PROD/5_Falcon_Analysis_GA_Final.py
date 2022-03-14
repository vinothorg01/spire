# Databricks notebook source
# dbutils.widgets.dropdown("social_platform_name", "facebook_page", ["facebook_page", "twitter"])
# dbutils.widgets.dropdown("run_type", "current", ["backfill", "current"])
# dbutils.widgets.dropdown("mode", "dev", ['dev','stg', 'prod'])


# COMMAND ----------

from falcon.utils.vault_utils import VaultAccess
from falcon.database.sqlalchemy_utils import SQLAlchemyUtils
from falcon.common import read_config, get_unique_brandnames_source, get_min_positive_after_diff

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sqlalchemy import MetaData
from falcon.utils.datetime_utils import AnchorDateTimeUtils, DateTimeUtils
from falcon.database.db_utils import DBUtils
from pyspark.sql.types import *
from pyspark.sql.functions import UserDefinedFunction
import pendulum
from falcon.utils.common_utils import CommonUtils



import logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

mode = dbutils.widgets.get("mode")
spark.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", False)


# COMMAND ----------

vt_rds_data = VaultAccess(mode=mode).get_settings(settings_type="rds")

# COMMAND ----------

engine = SQLAlchemyUtils().get_engine(vt_rds_data)
metadata_common = MetaData(schema="common")
# metadata_recs = MetaData(schema="recommendations")
metadata_settings = MetaData(schema="settings")
metadata_brand = MetaData(schema="brand")
metadata_settings = MetaData(schema='settings')
conn = engine.connect()
# db_utils = DBUtils(conn, engine, metadata_common = metadata_common, metadata_settings = metadata_settings)
db_utils = DBUtils.init_without_db_config(conn, engine, metadata_common = metadata_common, metadata_settings = metadata_settings)

brand_config = db_utils.get_all_brand_config()
content_config = read_config(settings_name  = 'content')
traffic_config = read_config(settings_name  = 'traffic')

# COMMAND ----------

# MAGIC %md # Google Analytics Traffic Analysis : Content Posted on Facebook using Socialflow

# COMMAND ----------

# MAGIC %md ## Tracked using UTM_CAMPAIGN

# COMMAND ----------

social_platform_name = dbutils.widgets.get("social_platform_name")
run_type = dbutils.widgets.get("run_type")

# COMMAND ----------

if run_type == "backfill":
  start_date = '2019-01-01'
#   end_date = pendulum.now(tz="America/New_York").subtract(days=1).to_date_string() # to allow 48 hours of traffic for GA
else:
  start_date = pendulum.now(tz="America/New_York").subtract(days=7).to_date_string() # to allow 48 hours of traffic for GA
#   end_date = pendulum.now(tz="America/New_York").subtract(days=1).to_date_string() # to allow 48 hours of traffic for GA

# COMMAND ----------

start_date

# COMMAND ----------

def get_unique_brandnames_source(brand_config, source1, source2=None):
    """
    Get the unique brand name based on the data source
    :param brand_config: complete brand configs from postgres across all brands
    :param source: datasource aleph_k2d/ga/sparrow
    :return: list of unique brand names on that particular datasource
    """
    if source2 is None:
      return list(set([x['brand_alias'][source1] for x in brand_config]))
    return list(set(list((x['brand_alias'][source1], x['brand_alias'][source2]) for x in brand_config)))

# COMMAND ----------

ga_map = get_unique_brandnames_source(brand_config, 'ga', 'ga_src_property_id')

# COMMAND ----------

ga_map

# COMMAND ----------

ga_brand_mapping = spark.createDataFrame(ga_map, ['brand', 'trackingid'])


# COMMAND ----------

# display(ga_brand_mapping)

# COMMAND ----------

ga_source_id = [row.trackingid for row in ga_brand_mapping.select('trackingid').collect()]
# ga_source_id

# COMMAND ----------

db_name = 'google' if mode == 'prod' else 'google_prod'


def ga_column_name_mapping():
  regexp = "^Session ID \(cd.*\)|^Query String \(cd.*\)|^Content ID \(cd.*\)"

  ga_cd = spark.table(f"{db_name}.ga_global_custom_dimensions")

  ga_cd = ga_cd.select('name', 'index', 'scope').where(F.col("name").rlike(regexp)).distinct()
  cd_mapping = dict()
  for each in ga_cd.collect():
    temp = list(each)
    name = '_'.join(temp[0].split()[:-1]).lower()
#     print(name)

    column = 'hits_customDimensions' if temp[2] == 'HIT' else 'customDimensions'
    column = column+'.'+str(temp[1])
#     print(column)

    cd_mapping[name] = column  
  return cd_mapping
cd_mapping = ga_column_name_mapping()


# COMMAND ----------

cd_mapping

# COMMAND ----------


ga_df = spark.table(f"{db_name}.google_analytics_global").where(F.col("logdate") >= start_date).where(F.col("sourcePropertyTrackingId").isin(ga_source_id))


# COMMAND ----------

ga_df = ga_df.withColumn('visitstarttime_est', F.from_utc_timestamp(F.to_timestamp(ga_df.visitStartTime), 'EST')).withColumn("visit_date", F.col("visitstarttime_est").cast(DateType())).where(F.col('visit_date') >= start_date)

# COMMAND ----------


ga_tot_df = ga_df.where(F.col("hits_type") == "PAGE")

# COMMAND ----------

# display(ga_tot_df)

# COMMAND ----------

if social_platform_name == "facebook_page":
  ga_df = ga_df.where(F.col("hits_type") == "PAGE").where(F.col("trafficSource_source").contains("facebook"))  
else:
  ga_df = ga_df.where(F.col("hits_type") == "PAGE").where(F.col("trafficSource_source").contains("twitter"))

ga_full_df = ga_df

# COMMAND ----------

if social_platform_name == "facebook_page":
  regexp = 'mbid=social_.*facebook|mbid=social_.*fb'
  ga_full_df = ga_full_df.withColumn("is_social_{}".format(social_platform_name), F.when(((F.col(cd_mapping['query_string']).rlike(regexp)) & (F.col(cd_mapping['query_string']).contains('utm_social-type=owned'))),1).otherwise(0)).withColumn("is_falcon", F.when(((F.col(cd_mapping['query_string']).contains("utm_campaign=falcon")) & (F.col(cd_mapping['query_string']).contains('utm_social-type=owned'))), 1).otherwise(0))
else:
  regexp = 'mbid=social_.*tw|mbid=social_.*twitter'
  ga_full_df = ga_full_df.withColumn("is_social_{}".format(social_platform_name), F.when(((F.col(cd_mapping['query_string']).rlike(regexp)) & (F.col(cd_mapping['query_string']).contains('utm_social-type=owned'))), 1).otherwise(0)).withColumn("is_falcon", F.when(((F.col(cd_mapping['query_string']).contains("utm_campaign=falcon")) & (F.col(cd_mapping['query_string']).contains('utm_social-type=owned'))), 1).otherwise(0))

# COMMAND ----------

ga_full_df = ga_full_df.select("logdate","visit_date", "is_social_{}".format(social_platform_name), "is_falcon",cd_mapping['content_id'],"sourcePropertyTrackingId", cd_mapping['session_id'], "visitStartTime").withColumnRenamed("26","content_id").withColumnRenamed("7","session_id")

# COMMAND ----------

ga_full_df = ga_full_df.join(ga_brand_mapping, ga_full_df.sourcePropertyTrackingId == ga_brand_mapping.trackingid, "inner")

# COMMAND ----------

# DBTITLE 1,#4a Distinct Page URLs(content_ids) accessed since October 1, as recorded by GA
distinct_pages = ga_full_df.groupBy("brand", "visit_date", "is_social_{}".format(social_platform_name), "is_falcon").agg(F.countDistinct(F.col("content_id")).alias("URLs_Accessed")).where(F.col("is_social_{}".format(social_platform_name)) == 1).orderBy(F.col("visit_date")).drop("is_social_{}".format(social_platform_name))

# COMMAND ----------

distinct_pages.write \
    .format("delta") \
    .partitionBy("brand", "visit_date") \
    .mode("overwrite") \
    .option("replaceWhere", "visit_date >= '{}'".format(start_date)) \
    .save("s3://cn-dse-falcon-{}/analysis/ga_distinct_social_{}_urls".format(mode, social_platform_name))


# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_ga_distinct_social_facebook_page_urls;
# CREATE TABLE falcon.analysis_ga_distinct_social_facebook_page_urls
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/ga_distinct_social_facebook_page_urls"

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_ga_distinct_social_twitter_urls;
# CREATE TABLE falcon.analysis_ga_distinct_social_twitter_urls
#   USING DELTA
#   LOCATION "s3://cn-falcon/analysis/ga_distinct_social_twitter_urls"

# COMMAND ----------

# MAGIC %md ### GA total DF

# COMMAND ----------

ga_tot_df = ga_tot_df.select("visit_date", cd_mapping['session_id'],"sourcePropertyTrackingId").join(ga_brand_mapping, ga_tot_df.sourcePropertyTrackingId == ga_brand_mapping.trackingid, "inner").withColumnRenamed("7", "session_id")

# COMMAND ----------

ga_total_df = ga_tot_df.groupBy("brand", "visit_date").agg(F.countDistinct("session_id").alias("ss"), F.count("session_id").alias("pvs")) # another way to generate the same results for sessions and pageviews

# COMMAND ----------

if social_platform_name == "facebook_page":
  ga_total_df.write \
      .format("delta") \
      .partitionBy("brand", "visit_date") \
      .mode("overwrite") \
      .option("replaceWhere", "visit_date >= '{}'".format(start_date)) \
      .save("s3://cn-dse-falcon-{}/analysis/ga_total_pageviews_sessions_all".format(mode))

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.ga_total_pageviews_sessions_all;
# CREATE TABLE falcon.analysis_ga_total_pageviews_sessions_all
#    USING DELTA
#    LOCATION "s3://cn-falcon/analysis/ga_total_pageviews_sessions_all"

# COMMAND ----------

content_config

# COMMAND ----------

aleph_k2d_brandnames = get_unique_brandnames_source(brand_config,"aleph_k2d")

# COMMAND ----------

final_content_df = spark.read.format("delta").load(content_config["data"]["s3_bucket"][mode]).where(F.col("brand").isin(aleph_k2d_brandnames))

# COMMAND ----------

joinType = "inner"
joinExpression = (ga_full_df["content_id"] == final_content_df["copilot_id"])

### THIS FOLLOWING JOIN EXPRESSION TAKES A LOT OF TIME (BE CAREFUL)
# joinExpression = ((ga_full_df["content_id"] == final_content_df["copilot_id"])|
#                   (F.expr("array_contains(df2.identifier_urls, df1.page_cleaned_url)")))
ga_content_df = ga_full_df.alias('df1').join(final_content_df.alias('df2'), joinExpression, joinType).select(['df1.*', 'df2.coll_pubdates', 'df2.coll_hfacs', 'df2.channel', 'df2.subchannel'])
# ga_content_df = ga_df.alias('df1').join(final_content_df.alias('df2'), joinExpression, joinType).select(['df1.*'])

# COMMAND ----------

# MAGIC %md ## GA FACEBOOK SESSIONS, PAGEVIEWS ALL

# COMMAND ----------

# DBTITLE 1,Social Pageviews combined with final_content_df (contains all brand content with all pubdates) - matching with copilot_ids
ga_sessions_pageviews_df = ga_content_df.groupBy("brand", "session_id", "visit_date") \
      .agg(F.max("is_social_{}".format(social_platform_name)).alias('is_social'), F.max('is_falcon').alias('is_falcon'), F.count('session_id').alias('pageviews')) \
      .where(F.col('is_social') > 0)

# COMMAND ----------

ga_sessions_pageviews_df = ga_sessions_pageviews_df.groupby("brand", "visit_date").agg(F.sum('is_social').alias('{}_sessions'.format(social_platform_name)), F.sum('is_falcon').alias('falcon_sessions'), F.sum('pageviews').alias('{}_pageviews'.format(social_platform_name)), F.sum(F.col('pageviews')*F.col('is_falcon')).alias('falcon_pageviews'))

# COMMAND ----------

ga_sessions_pageviews_df.write \
    .format("delta") \
    .partitionBy("brand", "visit_date") \
    .mode("overwrite") \
    .option("replaceWhere", "visit_date >= '{}'".format(start_date)) \
    .save("s3://cn-dse-falcon-{}/analysis/ga_organic_social_pageviews_sessions_{}_all".format(mode, social_platform_name))

# COMMAND ----------

# MAGIC %md ## #4c Analysis : Traffic from social facebook and how it has varied - Recently Published Content Excluded

# COMMAND ----------

ga_content_df = ga_content_df.withColumn("visitstarttime_utc", ga_content_df.visitStartTime.cast(TimestampType()))

# COMMAND ----------


ga_content_df = CommonUtils.get_definedhours_from_anchorpoint_pyspark(ga_content_df, output_colname="traffic_hours_from_anchor_point", to_ts_col="visitstarttime_utc", anchor_ts=traffic_config["anchor_date"])

# COMMAND ----------

get_min_positive_after_diff = UserDefinedFunction(get_min_positive_after_diff, IntegerType())

ga_content_df_with_hours_from_closest_pubdate = ga_content_df.withColumn("accessed_hours_from_closest_pubdate", get_min_positive_after_diff(ga_content_df["coll_hfacs"], ga_content_df["traffic_hours_from_anchor_point"]))

# COMMAND ----------

ga_content_df_excluded = ga_content_df_with_hours_from_closest_pubdate.where((F.col("accessed_hours_from_closest_pubdate") > 504))

# COMMAND ----------

# MAGIC %md ## Excluding recently posted content, traffic driven by Falcon is significantly more

# COMMAND ----------

ga_sessions_pageviews_df_excluded = ga_content_df_excluded.groupBy("brand", "session_id", "visit_date") \
      .agg(F.max("is_social_{}".format(social_platform_name)).alias('is_social'), F.max('is_falcon').alias('is_falcon'), F.count('session_id').alias('pageviews')) \
      .where(F.col('is_social') > 0)

# COMMAND ----------

ga_sessions_pageviews_df_excluded = ga_sessions_pageviews_df_excluded.groupby("brand", "visit_date").agg(F.sum('is_social').alias('{}_sessions'.format(social_platform_name)), F.sum('is_falcon').alias('falcon_sessions'), F.sum('pageviews').alias('{}_pageviews'.format(social_platform_name)), F.sum(F.col('pageviews')*F.col('is_falcon')).alias('falcon_pageviews'))

# COMMAND ----------

ga_sessions_pageviews_df_excluded.write \
    .format("delta") \
    .partitionBy("brand", "visit_date") \
    .mode("overwrite") \
    .option("replaceWhere", "visit_date >= '{}'".format(start_date)) \
    .save("s3://cn-dse-falcon-{}/analysis/ga_organic_social_pageviews_sessions_{}_older".format(mode, social_platform_name))

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_ga_organic_social_traffic_facebook_page_all;
# CREATE TABLE falcon.analysis_ga_organic_social_traffic_facebook_page_all
#    USING DELTA
#    LOCATION "s3://cn-falcon/analysis/ga_organic_social_pageviews_sessions_facebook_page_all"

# COMMAND ----------

# %sql 
# DROP TABLE IF EXISTS falcon.analysis_ga_organic_social_traffic_twitter_older;
# CREATE TABLE falcon.analysis_ga_organic_social_traffic_twitter_older
#    USING DELTA
#    LOCATION "s3://cn-falcon/analysis/ga_organic_social_pageviews_sessions_twitter_older"
