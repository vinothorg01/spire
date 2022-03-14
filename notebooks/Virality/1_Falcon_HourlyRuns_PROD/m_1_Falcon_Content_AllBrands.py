# Databricks notebook source
# dbutils.widgets.dropdown("mode", "dev", ['dev', 'prod'])

# COMMAND ----------

# DBTITLE 1,Imports
from falcon.utils.vault_utils import VaultAccess
from falcon.database.sqlalchemy_utils import SQLAlchemyUtils
from falcon.common import read_config, get_unique_brandnames_source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sqlalchemy import MetaData
from falcon.utils.datetime_utils import AnchorDateTimeUtils, DateTimeUtils
from falcon.database.db_utils import DBUtils
from pyspark.sql.types import *
from pyspark.sql.functions import UserDefinedFunction



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
content_config = read_config(settings_name="content")

# COMMAND ----------

# DBTITLE 1,UDF's
get_definedhours_from_anchorpoint = UserDefinedFunction(AnchorDateTimeUtils.get_definedhours_from_anchorpoint, IntegerType())

# COMMAND ----------

# MAGIC %md ## LIVE CONTENT CREATION

# COMMAND ----------

from api.aleph.content import published
from api.aleph.content import identifiers
from api.aleph.content import types

# COMMAND ----------

# MAGIC %md ### Process Published Data

# COMMAND ----------

aleph_k2d_brand_names = get_unique_brandnames_source(brand_config,'aleph_k2d')
aleph_k2d_brand_names

# COMMAND ----------

# DBTITLE 1,Remove any entity ids that have a revision with a publish datetime in the future
pub_df = published.load(revision='all').where(F.col('brand').isin(aleph_k2d_brand_names))
pub_df = pub_df.withColumn('pub_date_unixtime', F.unix_timestamp('pub_date'))
# ignore all the expired content
published_init_df = pub_df.where(F.col("expired") == False)
# # get the difference from the current unixtime to ignore content that has been scheduled to be published in the future
published_init_df = published_init_df.withColumn("diff_now_pubdate", F.unix_timestamp() - F.col("pub_date_unixtime"))
future_scheduled_df = published_init_df.where(F.col("diff_now_pubdate") < 0)
published_init_df = published_init_df.where(F.col("diff_now_pubdate") >= 0)
published_init_df = published_init_df.withColumnRenamed("id", "entity_id")

# COMMAND ----------

# MAGIC %md ### Get the Original Pubdates from the Published Init DF

# COMMAND ----------

# get the original pubdate for the entity ids (USE WITH CAUTION)
published_df_with_original_pubdate = published_init_df.groupBy("entity_id").agg(F.min("pub_date").alias("original_pubdate"))

# COMMAND ----------

# MAGIC %md ### Content HFACS 

# COMMAND ----------

published_init_df = published_init_df.withColumn('pub_date_ts', F.col('pub_date').cast(StringType()))
content_pubdates_hfac_df = published_init_df.withColumn("hours_from_anchor_point", get_definedhours_from_anchorpoint(F.lit(traffic_config["anchor_date"]), F.lit(F.col("pub_date_ts"))))
content_pubdates_hfac_df = content_pubdates_hfac_df.groupBy("entity_id").agg(F.collect_set("pub_date_ts").alias("coll_pubdates"), F.collect_set("hours_from_anchor_point").alias("coll_hfacs"))

# COMMAND ----------

# MAGIC %md ## Reading the live version number for the most recent published entity ids

# COMMAND ----------

# figure out the content id with the latest revision, as it wins
w_desc = Window().partitionBy("entity_id").orderBy("diff_now_pubdate", F.desc("revision"))
published_df_desc = published_init_df.withColumn("rank_desc", F.dense_rank().over(w_desc))
published_df_desc = published_df_desc.where(F.col("rank_desc") == 1)
published_df_desc = published_df_desc.drop_duplicates(subset = ["entity_id", "revision", "pub_date"])

# COMMAND ----------

# join the published df with the original pubdates obtained in the aggregations above
joinType = "inner"
joinExpression = (published_df_desc["entity_id"] == published_df_with_original_pubdate["entity_id"])
published_df = published_df_desc.alias('df1').join(published_df_with_original_pubdate.alias('df2'), joinExpression, joinType).select(['df1.*', 'df2.original_pubdate'])

# COMMAND ----------

joinType = "inner"
joinExpression = (published_df["entity_id"] == content_pubdates_hfac_df["entity_id"])
published_df = published_df.alias('df1').join(content_pubdates_hfac_df.alias('df2'), joinExpression, joinType).select(['df1.*', 'df2.coll_pubdates', 'df2.coll_hfacs'])

# COMMAND ----------

live_published_df = published_df.drop("diff_now_pubdate", "rank_desc")

# COMMAND ----------

# MAGIC %md ### Process Identifiers Data

# COMMAND ----------

identifiers_df = identifiers.load(revision='latest')
identifiers_df = identifiers_df.where(F.col('brand').isin(aleph_k2d_brand_names))
intermediate_identifiers_df = identifiers_df.groupBy("id").agg((F.collect_list(F.col("uri"))).alias("urls"), (F.collect_list(F.col("created_at"))).alias("created_at"), (F.collect_list(F.col("active"))).alias("active"))

# COMMAND ----------

def get_active_urls(urls, created_at, active):
  final_urls = {}
  for url, created_epoch_time, is_active in zip(urls, created_at, active):
    if url in final_urls:
      final_urls[url][created_epoch_time] = is_active
    else:
      final_urls[url] = {}
      final_urls[url][created_epoch_time] = is_active
  final_active_urls = []
  for url, url_dict in final_urls.items():
    if sorted(url_dict.items(), key=lambda x:x[0], reverse=True)[0][1] == True:  # don't change it to boolean
      final_active_urls.append(url)
  return final_active_urls
identifiers_udf = udf(get_active_urls, ArrayType(StringType()))

# COMMAND ----------

final_identifiers_df = intermediate_identifiers_df.select(F.col("id").alias("entity_id"), identifiers_udf("urls", "created_at", "active").alias("identifier_urls"))

# COMMAND ----------

# MAGIC %md #### Identifer x Published Information

# COMMAND ----------

joinType = "inner"
joinExpression = (final_identifiers_df["entity_id"] == live_published_df["entity_id"])
final_identifier_published_df = live_published_df.alias('df1').join(final_identifiers_df.alias('df2'), joinExpression, joinType).select(['df1.*', 'df2.identifier_urls'])

# COMMAND ----------

# MAGIC %md ### Load the content types

# COMMAND ----------

content_types = ['article', 'gallery', 'review', 'recipe']
final_df_columns = ["org_id",
                    "brand",
                    "collection_name",
                    "revision",
                    "id",
                    "hed",
                    "dek",
                    "body",
                    "tags",
                    "content_source",
                    "channel",
                    "sub_channel",
                    "seo_title",
                    "seo_description",
                    "social_title",
                    "social_description",
                   ]

# COMMAND ----------

for i, content_type_val in enumerate(content_types):
  if i == 0:
    content_df = types.load(collection_name=content_type_val, revision='all').where(F.col("brand").isin(aleph_k2d_brand_names)).select(*final_df_columns)
  else:
    content_df = content_df.union(types.load(collection_name=content_type_val, revision='all').where(F.col("brand").isin(aleph_k2d_brand_names)).select(*final_df_columns))

# COMMAND ----------

# DBTITLE 1,This is probably the expired entity_ids 
# THIS IS THE OLDER COMMAND, NOW WE JUST NEED TO COMBINE THE LIVE REVISIONS WITH HFACS AND IDENTIFIERS

joinType = "left_anti"
joinExpression = (content_df["id"] == final_identifier_published_df["entity_id"])
content_expired_df = content_df.alias('df1').join(final_identifier_published_df.alias('df2'), joinExpression, joinType).select(['df1.*'])

# COMMAND ----------

# THIS IS THE OLDER COMMAND, NOW WE JUST NEED TO COMBINE THE LIVE REVISIONS WITH HFACS AND IDENTIFIERS

joinType = "inner"
joinExpression = ((content_df["id"] == final_identifier_published_df["entity_id"]) & (content_df["revision"] == final_identifier_published_df["revision"]))
content_joined_df = content_df.alias('df1').join(final_identifier_published_df.alias('df2'), joinExpression, joinType).select(['df1.*', 'df2.identifier_urls', 'df2.pub_date', 'df2.coll_pubdates', 'df2.coll_hfacs', 'df2.original_pubdate'])
content_joined_df = content_joined_df.drop_duplicates(subset = ["id", "revision"])

# COMMAND ----------

content_joined_df = content_joined_df.withColumnRenamed("collection_name", "content_type")\
                                   .withColumnRenamed("id", "copilot_id")\
                                   .withColumnRenamed("hed", "headline")\
                                   .withColumnRenamed("sub_channel", "subchannel")\
                                   .withColumnRenamed("canonical_url", "content_url")\
                                   .withColumnRenamed("content_source", "contentsource")\
                                   .withColumnRenamed("social_title", "socialtitle")\
                                   .withColumnRenamed("social_description", "socialdescription")\
                                   .withColumn("pub_date_ts", F.col("pub_date").cast(StringType()))\
                                   .withColumn("pub_date_epoch", F.unix_timestamp("pub_date").cast(IntegerType()))\
                                   .withColumn("pub_date", F.col("pub_date_ts").cast(DateType()))\
                                   .withColumn("revision", F.col("revision").cast(IntegerType()))\
                                   .withColumn("original_pubdate_epoch", F.unix_timestamp("original_pubdate").cast(IntegerType())).drop("original_pubdate")         

# COMMAND ----------

content_data_path = content_config["data"]["s3_bucket"][mode]
content_data_path

# COMMAND ----------

content_joined_df.write.partitionBy("brand").format("delta").mode("overwrite").save(content_data_path)

# COMMAND ----------

# (content_joined_df.count())

# COMMAND ----------

# display(content_expired_df.groupBy("brand").agg(F.countDistinct("id")))

# COMMAND ----------


