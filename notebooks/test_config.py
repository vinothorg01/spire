# Databricks notebook source
# MAGIC %md
# MAGIC ####ADOPS MART PROJECT - DIMENSION PARAMETERS
# MAGIC #####THIS NOTEBOOK IS USED TO DEFINE THE PARAMETERS WHICH IS SHARED FOR ALL DIMENSIONS MODULES
# COMMAND ----------
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import *
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
import re
from pyspark.sql.window import Window
from pyspark.sql.functions import desc
from datetime import datetime,timedelta,date
import json
import requests as r
from pyspark.sql.types import *
import decimal
import operator
# COMMAND ----------
ENV=dbutils.widgets.get('ENV')
BUCKET_NAME=dbutils.widgets.get('BUCKET_NAME')
PORT=5432
print(ENV,BUCKET_NAME)
dbutils.widgets.text('EDW_HOSTNAME', '10.36.182.248')
dbutils.widgets.text('EDW_PORT', '1522')
dbutils.widgets.text('EDW_DATABASE', 'prd34')
dbutils.widgets.text('EDW_USERNAME', 'edw_ware_ro')
dbutils.widgets.text('EDW_PASSWORD', 'EDW_WAr3_R0#')
# dbutils.widgets.text('DATE', '2019-12-03')
# dbutils.widgets.text('PROCESS_DATE', '2019-12-04')
str ="jdbc:oracle:thin:@{}:{}:{}"
EDW_JDBC_URL=str.format(dbutils.widgets.get("EDW_HOSTNAME"),dbutils.widgets.get("EDW_PORT"),dbutils.widgets.get("EDW_DATABASE"))
# creating dictionary for each sales table
tables={
  "edw_date":"edw_ware.d_date",
  "edw_location":"edw_ware.d_location",
  "edw_orders":"edw_ware.d_order",
  "edw_lineitems":"edw_ware.d_order_line",
  "edw_brand":"edw_ware.d_Brand",
  "edw_account":"edw_ware.d_account"
}
fact_var_extract={
  "f_staq":{"load_type":"Initial","path":BUCKET_NAME + "/Facts/Staq/parquet"}
}
#date
# end=datetime.today() - timedelta(1)
initial_date="2018-01-01"
index_file_change_date="2019-08-28"
datadate_src_dict={}
tardis_lastrun_dict={
"dfp.network_impressions":[],
"dfp.network_backfill_impressions":[],
"dfp.network_clicks":[],
"dfp.network_backfill_clicks":[],
"dfp.deal":[],
"dfp.creative":[],
"dfp.line_items":[],
"dfp.orders":[],
"dfp.ad_units":[],
"dfp.advertisers":[],
"staq.amazon_pmp_data":[],
"staq.index_exchange_v2":[],
"staq.appnexus":[],
"staq.rubicon":[],
"staq.spotx":[],
"staq.triplelift":[],
"staq.index_exchange_buyer_brand":[],
"cnhw_ads.fact_staq":[],
"cnhw_ads.dim_advertiser":[],
"cnhw_ads.dim_brand" :[],
"cnhw_ads.dim_adunit":[], 
"cnhw_ads.dim_location":[], 
"cnhw_ads.dim_orders":[], 
"cnhw_ads.dim_lineitem":[],
"cnhw_staq.dim_deals":[],
"cnhw_ads.s_advertiser":[], 
"cnhw_ads.s_brand":[],
"cnhw_ads.s_location":[],
"cnhw_ads.dimension":[]
}
datadate_dict={
"staq.amazon_pmp_data":{"amazon_df":[]},
"staq.index_exchange_v2":{"indexv2_df":[]},
"staq.appnexus":{"appnexus_df":[]},
"staq.rubicon":{"rubicon_df":[]},
"staq.triplelift":{"triplelift_df":[]},
"staq.spotx":{"spotx_df":[]},
"staq.index_exchange_buyer_brand":{"indexbb_df":[]}               
}
# creating dictionary for each domain
sources_path_i = {"amazon_df" :"s3://cn-data-vendor/staq/parquet/amazon/pmp-data",
"appnexus_df" : "s3://cn-data-vendor/staq/parquet/appnexus",
"rubicon_df" : "s3://cn-data-vendor/staq/parquet/rubicon",
"spotx_df" : "s3://cn-data-vendor/staq/parquet/spotx",
"triplelift_df" : "s3://cn-data-vendor/staq/parquet/triple-lift",                  
"youtube_df":"s3://cn-data-vendor/youtube/analytics_reports/orc/ad_rates_rpt",
"impressions_df":"s3://cn-data-vendor/dfp/NetworkImpressions/orc",		
"clicks_df":"s3://cn-data-vendor/dfp/NetworkClicks/orc",		
"bfillimpressions_df":"s3://cn-data-vendor/dfp/data_transfer/NetworkBackfillImpressions/parquet",		
"bfillclicks_df":"s3://cn-data-vendor/dfp/NetworkBackfillClicks/orc",
"indexbb_df" : "s3://cn-data-vendor/staq/parquet/IndexExchangeBuyerBrand",
"indexv2_df":"s3://cn-data-vendor/staq/parquet/IndexExchangeV2",
"deals_df":"s3://cn-data-vendor/dfp/deals/orc",
"creatives_df":"s3://cn-data-vendor/dfp/creatives/orc"
}
# "bfillimpressions_df":"s3://cn-data-vendor/dfp/data_transfer/NetworkBackfillImpressions/parquet",
source_dict= {
"source_list_indexbb":[],
"source_list_indexv2":[],
"source_list_deals":[],
"source_list_bfillclicks":[],
"source_list_bfillimpressions":[],
"source_list_clicks":[],
"source_list_impressions":[],
"source_list_youtube":[],
"source_list_spotx":[],
"source_list_rubicon":[],
"source_list_appnexus":[],
"source_list_amazon":[],
"source_list_triplelift":[]
}
skip_sources={}
FACT_OUTPUT_PATH=BUCKET_NAME + "/Facts/youtube/parquet"
up_stnd_src_dict={
"up_adunit_df": BUCKET_NAME +"/standardization/{}/market=us/business_mapping/dim_adsizes/ad_size.json".format(ENV),
"up_deals_df": BUCKET_NAME +"/standardization/{}/market=us/business_mapping/deal/deal.json".format(ENV),
"stnd_region_df": BUCKET_NAME +"/standardization/{}/market=us/business_mapping/country_dim".format(ENV), 
"stnd_advertiser_df": BUCKET_NAME +"/standardization/{}/market=us/advertiser/mapped/parquet".format(ENV),
"stnd_location_df": BUCKET_NAME +"/standardization/{}/market=us/location/mapped/parquet".format(ENV),
"stnd_brand_df": BUCKET_NAME +"/standardization/{}/market=us/brand/mapped/parquet".format(ENV)
}
sources_path_f={
  "order_df":"s3://cn-data-vendor/dfp/orders/orc",
  "lineItems_df":"s3://cn-data-vendor/dfp/line_items/orc",
  "adUnit_df":"s3://cn-data-vendor/dfp/ad_units/orc",
  "advertiser_df":"s3://cn-data-vendor/dfp/advertisers/orc"
}
if ENV == 'dev':
  TARDIS_API_PROCESS_STATUS_ENDPOINT='https://tardis-api.k8s.us-east-1--nonproduction.containers.aws.conde.io/api/process-status/'
  TARDIS_API_DATA_AVAILABILITY_ENDPOINT= 'https://tardis-api.k8s.us-east-1--nonproduction.containers.aws.conde.io/api/data-availability/'
  TARDIS_API_TOKEN='315e1ca79ad5d373c9b35a2615a07928fee58697'
  HOST_NAME = "parrot-dev.codbdbe5bplh.us-east-1.rds.amazonaws.com"
  DATABASE="testdb"
  CONT_DETAILS={ "user": "parrotdev", "password": "P@rSTG#2018" }
  
elif ENV == 'prod':
  TARDIS_API_PROCESS_STATUS_ENDPOINT='https://tardis.conde.io/api/process-status/'
  TARDIS_API_DATA_AVAILABILITY_ENDPOINT='https://tardis.conde.io/api/data-availability/'
  TARDIS_API_TOKEN='3c981519297e7f1cc5e545644d8fffa3eeeb2faf'
  HOST_NAME="data-availability.cjholbtpqqsr.us-east-1.rds.amazonaws.com"
  DATABASE="datavail"
  CONT_DETAILS={ "user": "datvailusr", "password": "d@t3yPRD#19" }
  
URL="jdbc:postgresql://{0}:{1}/{2}".format(HOST_NAME,PORT,DATABASE)
 
tables_dict={"DATA_AVBLTY":"tardis.data_availability",
             "STS_LOOKUP":"tardis.status_lookup"}