# Databricks notebook source

# dbutils.widgets.dropdown("mode", "dev", ['dev', 'prod']) 

# COMMAND ----------


# COMMAND ----------

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
#get Mode
mode = dbutils.widgets.get("mode")

# COMMAND ----------

from falcon.utils.vault_utils import VaultAccess
from falcon.database.sqlalchemy_utils import SQLAlchemyUtils
# from falcon.utils.s3fs_utils import S3FSActions
from falcon.common import read_config, get_unique_brandnames_source
from falcon.database.db_utils import DBUtils
from sqlalchemy import MetaData
from falcon.utils.datetime_utils import AnchorDateTimeUtils
import pendulum
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.functions import UserDefinedFunction

# COMMAND ----------

vt_rds_data = VaultAccess(mode=mode).get_settings(settings_type="rds")


## Databases Used

engine = SQLAlchemyUtils().get_engine(vt_rds_data)
metadata_common = MetaData(schema="common")

metadata_settings = MetaData(schema="settings")
conn = engine.connect()

db_utils = DBUtils.init_without_db_config(conn, engine, metadata_common = metadata_common, metadata_settings = metadata_settings)

# COMMAND ----------

traffic_config = read_config(settings_name="traffic")
brand_config = db_utils.get_all_brand_config()

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

# COMMAND ----------

confluents3_bucket = traffic_config["sparrow"]["confluent"]["s3_bucket"][mode]

# COMMAND ----------

# DBTITLE 1,Add "_" instead of "-" for brand names for confluent brand names
sparrow_brand_names = get_unique_brandnames_source(brand_config,"sparrow")
confluent_brand_names = [brand.replace("-", "_") for brand in sparrow_brand_names]

# COMMAND ----------

# sparrow_brand_names

# COMMAND ----------

# confluent_brand_names

# COMMAND ----------

end_hour = AnchorDateTimeUtils.get_currenthours_from_anchorpoint(traffic_config["anchor_date"])
start_hour = end_hour - traffic_config["default_hours"]
date_filter = pendulum.parse(traffic_config["anchor_date"]).add(hours=start_hour).to_date_string()
get_definedhours_from_anchorpoint = UserDefinedFunction(AnchorDateTimeUtils.get_definedhours_from_anchorpoint, IntegerType())

# COMMAND ----------

traffic_data_pq_df = spark.readStream.format("delta")\
                                .option("ignoreChanges", "true")\
                                .option("maxFilesPerTrigger", 20000000)\
                                .option("maxBytesPerTrigger","80g")\
                                .load(confluents3_bucket)\
                                .where(F.col("dt") >= date_filter)\
                                .where(F.lower("type") == "pageview")\
                                .where(F.lower("brand").isin(confluent_brand_names))

# COMMAND ----------

traffic_data_pq_df = traffic_data_pq_df.select(["brand", "_ts", "cid", "xid", "prt", "prs"])\
                                .withColumn("timestamp_utc", F.col("_ts").cast(TimestampType()))\
                                .withColumn("hours_from_anchor_point", get_definedhours_from_anchorpoint(F.lit(traffic_config["anchor_date"]), F.lit(F.col("_ts")))).drop("_ts")\
                                .withColumn("brand", F.regexp_replace("brand", "_", "-")) #change back to initial sparrow column names 

# COMMAND ----------

s3location = traffic_config["sparrow"]["data"]["s3_bucket"][mode]
checkpointLocation = traffic_config["sparrow"]["data"]["checkpoints"][mode]

# COMMAND ----------

checkpointLocation

# COMMAND ----------

s3location

# COMMAND ----------

traffic_data_pq_df.writeStream\
  .format("delta")\
  .outputMode("append")\
  .partitionBy("brand", "hours_from_anchor_point")\
  .option("checkpointLocation", checkpointLocation)\
  .queryName("sparrow_traffic_stream")\
  .start(s3location)

# COMMAND ----------


