# Databricks notebook source

# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,Get the mode here Dev/Prod
mode = dbutils.widgets.get("Mode")
brand_name = dbutils.widgets.get("brand_name")
brand_id = dbutils.widgets.get("brand_id")

# COMMAND ----------

import falcon
from falcon.utils.vault_utils import VaultAccess
from falcon.database.sqlalchemy_utils import SQLAlchemyUtils
from falcon.database.db_utils import DBUtils
from sqlalchemy import MetaData, Table, Column, String, JSON, ForeignKey, UniqueConstraint
import pandas as pd
from uuid import uuid4
from sqlalchemy.dialects.postgresql import UUID

# COMMAND ----------

falcon.__version__

# COMMAND ----------

vt_aws_data = VaultAccess(mode=mode).get_settings(settings_type="aws")
vt_rds_data = VaultAccess(mode=mode).get_settings(settings_type="rds")

# COMMAND ----------

# DBTITLE 1,Connecting to the databases
engine = SQLAlchemyUtils().get_engine(vt_rds_data)
metadata_settings = MetaData(schema="settings")
metadata_common = MetaData(schema="common")
conn = engine.connect()

db_utils = DBUtils.init_without_db_config(conn, engine, metadata_common = metadata_common, metadata_settings = metadata_settings)

# COMMAND ----------

accountid_fb = db_utils.get_social_platform_id("facebook_page")
accountid_tw = db_utils.get_social_platform_id("twitter")
print(accountid_fb)
print(accountid_tw)

# COMMAND ----------

data = []
records_fb = {}
records_fb['brand_alias'] = {'brand_name': 'test_brand',
                             'ga': '',
                             'sparrow': '',
                             'brand_url': '',
                             'url_shortener': '',
                             'organization_id': '',
                             'aleph_k2d': ''}
records_fb['content'] = {
    "min_pub_date_filter": "",
    "min_pub_date_year_threshold": 0,
    "considered_content": {
        "old_content": 0,
        "new_content": 0
    }
}

records_fb['traffic'] = {
    "previous_traffic_hours": 0,
    "traffic_hours": 0
}
records_fb['falcon'] = {
    "went_live": "",
    "output_content_type_min_counts": {
        "story_recipes": 0,
        "gallery_reviews": 0
    }
}
records_fb['socialflow'] = {
    "account_type": "facebook_page",
    "brand_name_to_publish": {
        "dev": "Falcon Test",
        "prod": "test"
    },
    "brand_name_for_queue": "Test",
    "shelf_life": 0,
    "posted_data_hours": 0,
    "posted_data_brand": "",
    "falcon_labels": {
        "falcon_never": {
            "recommend_after_hours": 0
        },
        "falcon_later": {
            "recommend_after_hours": 0
        }
    },
    "publish_message": [
        "socialtitle",
        "socialcopy",
        "social_created_epoch_time",
        "content_url",
        "pub_date_epoch"
    ],
    "content_attributes_desc": {
        "link": "content_url",
        "name": "socialtitle",
        "description": "socialdescription"
    }
}
records_fb['brand_name'] = "test_brand"
records_fb['social_platform_name'] = "facebook_page"
records_fb['brand_id'] = "1234abcd"
records_fb['social_platform_id'] = accountid_fb

# COMMAND ----------

records_tw = {}
records_tw['brand_alias'] = {'brand_name': 'test_brand',
                             'ga': '',
                             'sparrow': '',
                             'brand_url': '',
                             'url_shortener': '',
                             'organization_id': '',
                             'aleph_k2d': ''}
records_tw['content'] = {
    "min_pub_date_filter": "",
    "min_pub_date_year_threshold": 0,
    "considered_content": {
        "old_content": 0,
        "new_content": 0
    }
}

records_tw['traffic'] = {
    "previous_traffic_hours": 0,
    "traffic_hours": 0
}
records_tw['falcon'] = {
    "went_live": "",
    "output_content_type_min_counts": {
        "story_recipes": 0,
        "gallery_reviews": 0
    }
}
records_tw['socialflow'] = {
    "account_type": "twitter",
    "brand_name_to_publish": {
        "dev": "",
        "prod": ""
    },
    "brand_name_for_queue": "Test",
    "shelf_life": 0,
    "posted_data_hours": 0,
    "posted_data_brand": "",
    "falcon_labels": {
        "falcon_never": {
            "recommend_after_hours": 0
        },
        "falcon_later": {
            "recommend_after_hours": 0
        }
    },
    "publish_message": [
        "socialtitle",
        "socialcopy",
        "social_created_epoch_time",
        "content_url",
        "pub_date_epoch"
    ],
    "content_attributes_desc": {
        "link": "content_url",
        "name": "socialtitle",
        "description": "socialdescription"
    }
}
records_tw['brand_name'] = "test_brand"
records_tw['social_platform_name'] = "twitter"
records_tw['brand_id'] = "1234abcd"
records_tw['social_platform_id'] = accountid_tw

# COMMAND ----------

data.append(records_fb)
data.append(records_tw)

# COMMAND ----------

brand_settings_data = pd.json_normalize(data, max_level=0).to_dict(orient="records")
brand_settings_data

# COMMAND ----------

brand_details_data = {
    'brand_id': brand_id,
    'brand_name': brand_name
}
brand_details_data

# COMMAND ----------

brand_details = Table('brand_details', metadata_common, autoload=True, autoload_with=engine)
config = Table('config', metadata_settings, autoload=True, autoload_with=engine)

# COMMAND ----------

conn.execute(brand_details.insert(), brand_details_data)
conn.execute(config.insert(), brand_settings_data)

# COMMAND ----------

print("Records inserted")
