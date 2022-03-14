# Databricks notebook source
# dbutils.widgets.dropdown("Mode", "dev", ['dev', 'prod'])
# dbutils.widgets.dropdown("Brand_Name", "self", ['self', 
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
# dbutils.widgets.dropdown("Social_Account_Type", "facebook_page", ["facebook_page", "twitter"])

# COMMAND ----------

from falcon.utils.vault_utils import VaultAccess
from falcon.database.sqlalchemy_utils import SQLAlchemyUtils
from falcon.database.db_utils import get_social_platform_uuid, get_perbrand_settings
from sqlalchemy import MetaData

# COMMAND ----------

mode = dbutils.widgets.get("Mode")
brand_name = dbutils.widgets.get("Brand_Name")
social_account_type = dbutils.widgets.get("Social_Account_Type")

# COMMAND ----------

vt_aws_data = VaultAccess(mode=mode).get_settings(settings_type="aws")
vt_rds_data = VaultAccess(mode=mode).get_settings(settings_type="rds")

# COMMAND ----------

engine = SQLAlchemyUtils().get_engine(vt_rds_data)
metadata_public = MetaData(schema="public")
metadata_brand_settings = MetaData(schema="brand_settings")
conn = engine.connect()

# COMMAND ----------

def read_config_from_postgres(brand_name,account_type,metadata_brand_settings,metadata_public,engine):
  account_id = get_social_platform_uuid(conn, account_type, metadata_public, engine)
  return get_perbrand_settings(conn,brand_name, account_id[1],metadata_brand_settings,engine)

# COMMAND ----------

prediction_brand_config = read_config_from_postgres(brand_name,social_account_type,metadata_brand_settings,metadata_public,engine)

# COMMAND ----------

prediction_brand_config

# COMMAND ----------

actual_brand_alias = {        'brand_name': "allure", # same as the key for s3 buckets partition
        'ga': "allure",
        'sparrow': "allure",
        'brand_url': "allure.com",
        'url_shortener': "in.allure.com",
        'organization_id': "4gKgcEzcAZvqb9t5pcZHNaDvXACy",
        'aleph_k2d': "allure"
}
assert actual_brand_alias == prediction_brand_config['brand_alias']

# COMMAND ----------

actual_content = {'min_pub_date_filter': '2018-01-01',
  'considered_content': {'old_content': 504, 'new_content': 48}}
assert actual_content == prediction_brand_config['content']

# COMMAND ----------

actual_traffic = {'previous_traffic_hours': 3, 'traffic_hours': 10}
assert actual_traffic == prediction_brand_config['traffic']

# COMMAND ----------

actual_socialflow = {'account_type': 'facebook_page',
  'brand_name_to_publish': {'dev': 'Falcon Test', 'prod': 'Allure'},
  'brand_name_for_queue': 'Allure',
  'shelf_life': 24,
  'posted_data_hours': 336,
  'posted_data_brand': 'Allure',
  'falcon_labels': {'falcon_never': {'recommend_after_hours': 'Infinite'},
   'falcon_later': {'recommend_after_hours': 336}},
  'publish_message': ['socialtitle',
   'socialcopy',
   'social_created_epoch_time',
   'content_url',
   'pub_date_epoch'],
  'content_attributes_desc': {'link': 'content_url',
   'name': 'socialtitle',
   'description': 'socialdescription'}}
assert actual_socialflow == prediction_brand_config['socialflow']

# COMMAND ----------

actual_falcon = {'went_live': '2019-10-18',
  'output_content_type_min_counts': {'story_recipes': 1,
   'gallery_reviews': 1}}
assert actual_falcon == prediction_brand_config['falcon']

# COMMAND ----------


