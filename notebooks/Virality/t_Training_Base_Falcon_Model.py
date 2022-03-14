# Databricks notebook source
dbutils.library.installPyPI("scikit-learn", version="0.22.2")
dbutils.library.installPyPI("xgboost")
dbutils.library.restartPython()

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

brand_name = dbutils.widgets.get("brand_name")
social_platform_name = dbutils.widgets.get("social_platform_name")
mode = dbutils.widgets.get("mode")
social_partner_name = dbutils.widgets.get("social_partner_name")
data_for = dbutils.widgets.get("data_for")

if data_for=="train":
  is_train = True
else:
  is_train=False

# COMMAND ----------

import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor, XGBClassifier
import warnings

# # from causalml.inference.meta import XGBTLearner, MLPTLearner
# from causalml.inference.meta import BaseSRegressor, BaseTRegressor, BaseXRegressor, BaseRRegressor
# from causalml.inference.meta import BaseSClassifier, BaseTClassifier, BaseXClassifier, BaseRClassifier
# from causalml.inference.meta import LRSRegressor
# from causalml.match import NearestNeighborMatch, MatchOptimizer, create_table_one
# from causalml.propensity import compute_propensity_score, GradientBoostedPropensityModel, ElasticNetPropensityModel
# from causalml.dataset import *
# from causalml.metrics import *

warnings.filterwarnings('ignore')
plt.style.use('fivethirtyeight')
pd.set_option('display.float_format', lambda x: '%.4f' % x)

# imports from package
import logging
from sklearn.dummy import DummyRegressor
from sklearn.metrics import mean_squared_error as mse
from sklearn.metrics import mean_absolute_error as mae
import statsmodels.api as sm
from copy import deepcopy


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

import pyspark.sql.functions as F
import mlflow
import mlflow.pyfunc

# COMMAND ----------

# DBTITLE 1,Wrapper for MLFlow Models
class CausalMLModelWrapper(mlflow.pyfunc.PythonModel):
  def __init__(self, model):
    self.model = model

# COMMAND ----------

# MAGIC %md ## Training Data for the Brands

# COMMAND ----------

# %run ./m_3_Falcon_Feature_Generation_Brands

# COMMAND ----------

# vt_aws_data = VaultAccess(mode=mode).get_settings(settings_type="aws")
vt_rds_data = VaultAccess(mode=mode).get_settings(settings_type="rds")
vt_socialplatform_data = VaultAccess(mode=mode).get_settings(settings_type=f"{social_partner_name}")

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

hfac = AnchorDateTimeUtils.get_currenthours_from_anchorpoint(
        traffic_config["anchor_date"]
    )


print(f"Current hour from anchorpoint is {hfac}")
logger.info(f"Current hour from anchorpoint is {hfac}")

# COMMAND ----------

from falcon.datareader.sparrow_reader import SparrowDataReader
sparrow_data_reader = SparrowDataReader(is_train=is_train, 
                                        brand_name=brand_name,
                                        hfac=hfac, 
                                        social_account_type=social_platform_name,
                                        mode=mode,
                                        brand_config = prediction_brand_config,
                                       spark=spark)
sparrow_df,_,_ = sparrow_data_reader.read_sparrow_data()

# COMMAND ----------

from falcon.datareader.content_reader import ContentDataReader
content_data_reader = ContentDataReader(brand_config =prediction_brand_config,
                                        mode=mode,
                                       spark=spark)
content_df, identifiers_urls = content_data_reader.read_content_data()

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
social_brand_content_df, _ = socialcopy_data_reader.get_socialposted_brand_data(identifiers_urls)

# COMMAND ----------

from falcon.featuregenerator.feature_generator import FeatureGenerator
feature_generator = FeatureGenerator(prediction_brand_config, 
                                     logger
                                     )
sparrow_content_final_df= feature_generator.sparrow_content_socialflow_data(sparrow_df, final_content_df, social_brand_content_df)
final_df = feature_generator.remove_published_before(sparrow_content_final_df)
checker = feature_generator.add_features_to_df(final_df)

# COMMAND ----------

# DBTITLE 1,To check the minimum dates from which social copy data is available
# check = spark.read.format("delta").load('s3://cn-falcon/data_delta/socialcopy_data/facebook_page')
# display(check.where(F.col("brand") == "gq-germany").select(F.min("social_created_date")))

# COMMAND ----------

# MAGIC %md ## Loading Training Data with Feature Generation

# COMMAND ----------

training_data = checker

# COMMAND ----------

display(training_data)

# COMMAND ----------

# display(training_data.where(F.col("final_social_posted") == 1))

# COMMAND ----------

# DBTITLE 1,Posted Social Data Counts
# display(training_data.groupBy("final_social_posted").count())

# COMMAND ----------

training_data.columns

# COMMAND ----------

training_data = training_data.drop('anon_visits_prev_3hrs_2hrs', 'anon_visits_prev_2hrs_1hrs', 'anon_visits_prev_1hrs', 'total_anon_visits_prev_1hrs_3hrs')

# COMMAND ----------

# training_data.count()

# COMMAND ----------

# display(training_data.select(F.min("hours_from_anchor_point"), F.max("hours_from_anchor_point")))

# COMMAND ----------

display(training_data.groupby("final_social_posted").count())

# COMMAND ----------

# DBTITLE 1,First Filter: Sum of last 3 hours of traffic is at least 5 page views, so that its just not a random visit(can experiment with this further)
training_data_sum_3hr_pvs = training_data.withColumn("sum_total_pageviews_last_3hrs", (F.col("total_pageviews_prev_3hrs_2hrs") + F.col("total_pageviews_prev_2hrs_1hrs") + F.col("total_pageviews_prev_1hrs")))

# COMMAND ----------

display(training_data_sum_3hr_pvs.where(F.col('sum_total_pageviews_last_3hrs') < 5))

# COMMAND ----------

training_data_sum_3hr_pvs = training_data_sum_3hr_pvs.where(F.col('sum_total_pageviews_last_3hrs') >= 5)

# COMMAND ----------

social_posted_training_df = training_data_sum_3hr_pvs.where(F.col("final_social_posted") == 1)
social_posted_training_df.count()

# COMMAND ----------

non_social_posted_training_df = training_data_sum_3hr_pvs.where(F.col("final_social_posted") == 0)
non_social_posted_training_df.count()

# COMMAND ----------

# non_social_posted_training_df_sample = non_social_posted_training_df.sample(withReplacement=False, fraction=0.1, seed=42)
# non_social_posted_training_df_sample.count()

# COMMAND ----------

final_sampled_df = social_posted_training_df.union(non_social_posted_training_df).select(['hours_from_anchor_point', 'cid', 'total_hourly_events_pageviews',
       'anon_visits', 'direct_visits', 'referral_visits', 'internal_visits',
       'search_visits', 'search_yahoo_visits', 'search_bing_visits',
       'search_google_visits', 'social_visits', 'social_facebook_visits',
       'social_instagram_visits', 'social_twitter_visits',
       'total_pageviews_prev_3hrs_2hrs', 'total_pageviews_prev_2hrs_1hrs',
       'total_pageviews_prev_1hrs', 'total_pageviews_next_6hrs',
       'anon_visits_next_6hrs', 'social_visits_prev_3hrs_2hrs',
       'social_visits_prev_2hrs_1hrs', 'social_visits_prev_1hrs',
       'social_visits_next_6hrs', 'social_facebook_visits_prev_3hrs_2hrs',
       'social_facebook_visits_prev_2hrs_1hrs',
       'social_facebook_visits_prev_1hrs', 'social_facebook_visits_next_6hrs',
       'total_pageviews', 'average_hourly_events', 'final_social_posted',
       'content_type', 'channel', 'subchannel',
       'pubdate_diff_anchor_point',
       'content_recency_when_accessed', 
       'sum_total_pageviews_last_3hrs'])
# final_sampled_df.count()

# COMMAND ----------

final_sampled_df.count()

# COMMAND ----------

df = final_sampled_df.toPandas()

# COMMAND ----------

df.shape

# COMMAND ----------

anchor_date = traffic_config["anchor_date"]

# COMMAND ----------

features = ['total_pageviews_prev_3hrs_2hrs', 'total_pageviews_prev_2hrs_1hrs',
       'total_pageviews_prev_1hrs', 'social_visits_prev_3hrs_2hrs',
       'social_visits_prev_2hrs_1hrs', 'social_visits_prev_1hrs',
       'content_type', 'content_recency_when_accessed',  'final_social_posted', 'total_pageviews_next_6hrs']

# COMMAND ----------

df_with_features = df.loc[:, features]

# COMMAND ----------

df_with_features.pivot_table(values="total_pageviews_next_6hrs", index="final_social_posted", aggfunc=[np.mean, np.size], margins=True)

# COMMAND ----------

# DBTITLE 1,Creating Training and Testing Datasets
treatment = np.array(['treatment' if val==1 else 'control' for val in df_with_features["final_social_posted"].values])
df_with_features.loc[:, "treatment"] = treatment
df_train, df_test = train_test_split(df_with_features, train_size=0.7, random_state=111, stratify = df_with_features["final_social_posted"].values)

# COMMAND ----------

df_train["final_social_posted"].value_counts()

# COMMAND ----------

df_test["final_social_posted"].value_counts()

# COMMAND ----------

df_train_copy = df_train.copy()
df_test_copy = df_test.copy()

# COMMAND ----------

# MAGIC %md ## Create training data for the Meta Learners

# COMMAND ----------

# MAGIC %md ## Feature Selection with Filter Methods

# COMMAND ----------

# from causalml.feature_selection.filters import FilterSelect

# COMMAND ----------

# filter_f = FilterSelect()

# COMMAND ----------

df_train

# COMMAND ----------

np.asarray(df_train)

# COMMAND ----------

treatment_indicator = "final_social_posted"
y_name = "total_pageviews_next_6hrs"

# COMMAND ----------

features = list(df_train.columns)
features.remove(treatment_indicator)
features.remove(y_name)

# COMMAND ----------

features.remove("treatment")

# COMMAND ----------

features

# COMMAND ----------

df_train = df_train.astype({'content_type': int})#, 'hfac_to_dtIs_month_end': int, 'hfac_to_dtIs_month_start': int, 'hfac_to_dtIs_quarter_end': int, 'hfac_to_dtIs_year_end': int, 'hfac_to_dtIs_year_start': int})
df_test = df_test.astype({'content_type': int})#, 'hfac_to_dtIs_month_end': int, 'hfac_to_dtIs_month_start': int, 'hfac_to_dtIs_quarter_end': int, 'hfac_to_dtIs_year_end': int, 'hfac_to_dtIs_year_start': int})

# COMMAND ----------

df_train

# COMMAND ----------

# method = 'F'
# f_imp = filter_f.get_importance(data=df_train, features=features, y_name=y_name, method=method, experiment_group_column='treatment', control_group = 'control', treatment_group='treatment', n_bins=10)
# f_imp

# COMMAND ----------

# final_features = list(f_imp.loc[f_imp["p_value"] <= 0.9, "feature"].values)
final_features = features

# COMMAND ----------

# MAGIC %md ## Falcon Model to compute propensity scores

# COMMAND ----------

df_train.final_social_posted.value_counts()

# COMMAND ----------

df_test.final_social_posted.value_counts()

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier

# COMMAND ----------

def get_sample(df, n, seed=42):
  """ Gets a random sample of n rows from df, without replacement.
  Parameters:
  -----------
  df: A pandas data frame, that you wish to sample from.
  n: The number of rows you wish to sample.
  Returns:
  --------
  return value: A random sample of n rows of df.
  Examples:
  ---------
  >>> df = pd.DataFrame({'col1' : [1, 2, 3], 'col2' : ['a', 'b', 'a']})
  >>> df
     col1 col2
  0     1    a
  1     2    b
  2     3    a
  >>> get_sample(df, 2)
     col1 col2
  1     2    b
  2     3    a
  """
  idxs = sorted(np.random.RandomState(seed=seed).permutation(df.shape[0])[:n])
  return df.iloc[idxs].copy()

# COMMAND ----------

def return_balanced_data(df, sample_size, seed=42):
  negative_samples = df.loc[df["final_social_posted"] == 0, :]
  positive_samples = df.loc[df["final_social_posted"] == 1, :]
  positive_current_sample = get_sample(positive_samples, n=sample_size, seed=seed)
  negative_current_sample = get_sample(negative_samples, n=sample_size, seed=seed)
  final_df_current = pd.concat([positive_current_sample, negative_current_sample], axis=0)
  return final_df_current

# COMMAND ----------

for i in range(10):
  df_train_sampled = return_balanced_data(df_train, sample_size=df_train.loc[df_train["final_social_posted"] == 1, :].shape[0], seed=i)
  df_test_sampled = return_balanced_data(df_test, sample_size=df_test.loc[df_test["final_social_posted"] == 1, :].shape[0], seed=i)

# COMMAND ----------

df_train_sampled.shape

# COMMAND ----------

# DBTITLE 1,Propensity Score using current Falcon Model
from sklearn.pipeline import make_pipeline

# COMMAND ----------

def model_falcon(df_t, actions_t, y_t):
    df, actions, y = df_t.copy(), actions_t.copy(), y_t.copy()
    outcomes = np.array(y)
    X = df.reset_index(drop=True)
    unique_actions, action_counts = np.unique(actions, return_counts=True)
    total_len = len(actions)
    bias = {a: c / total_len for a, c in zip(unique_actions, action_counts)}
    biases = np.array([bias[a] for a in actions])
    weights = outcomes / biases
    pipe = make_pipeline(StandardScaler(), LogisticRegression(solver="lbfgs", max_iter=500, C=50))
    pipe.fit(X, actions, **{'logisticregression__sample_weight': weights})
    return pipe

# COMMAND ----------

# DBTITLE 1,Comment these to avoid the under sampling done in the 3-5 lines above this line
# df_train = df_train_sampled
# df_test = df_test_sampled

# COMMAND ----------

clip_bounds=(1e-3, 1 - 1e-3)

# COMMAND ----------

algo = "logistic_prod"
experiment_name = f'brand={brand_name}/social_platform={social_platform_name}/algo={algo}'

with mlflow.start_run(run_name=experiment_name) as run:
  
  clf = model_falcon(df_t=df_train[final_features], actions_t=df_train[treatment_indicator], y_t=df_train[y_name])

  # Log the sklearn model and register as version 1
  mlflow.sklearn.log_model(
      sk_model=clf,
      artifact_path=f"{brand_name}_{social_platform_name}_{algo}",
      registered_model_name=f"falcon_{brand_name}_{social_platform_name}_{algo}"
  )
  mlflow.set_tags({"Project":"Falcon", 
                   "Group":"Data Science"})

# COMMAND ----------

from sklearn.metrics import roc_auc_score as auc

# COMMAND ----------

# DBTITLE 1,Current Model
auc(df_train[treatment_indicator], clf.predict_proba(df_train[final_features])[:, 1])

# COMMAND ----------

auc(df_test[treatment_indicator], clf.predict_proba(df_test[final_features])[:, 1])

# COMMAND ----------


