from spire.config import config

ID_VAR = "xid"
FEATURES_ID_VAR = "infinity_id"
FEATURES_DATE_VAR = "last_seen_date"

SPIRE_BUCKET = config.SPIRE_ENVIRON

# TODO: move this to config and make configurable through ENVs
FEATURE_BUCKET = "cn-aleph"
FEATURE_KEY = "featurestore/rolling_spire"

DELTA_FORMAT = "delta"
PARQUET_FORMAT = "parquet"
PARQUET_OUTPUT_URI = f"s3a://{SPIRE_BUCKET}/output/scores"
DELTA_OUTPUT_URI = f"s3a://{SPIRE_BUCKET}/output/delta/scores"

MLFLOW_INTERMEDIATE_OUTPUT_URI = f"s3a://{SPIRE_BUCKET}/data/intermediates/mlflow"
