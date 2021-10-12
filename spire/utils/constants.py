from spire.version import __safe_version__
from spire.config import config

# spire infra task and DAG aliases
ASSEMBLE_TASK_ID = "assembly"
SCORE_TASK_ID = "scoring"
TARGETS_TASK_ID = "target"
TRAIN_TASK_ID = "training"

# Databricks API constants
GET_CLUSTERS_API = b"/api/2.0/clusters/get"
LIST_CLUSTERS_API = b"/api/2.0/clusters/list"
CREATE_CLUSTER_API = b"/api/2.0/clusters/create"
START_CLUSTER_API = b"/api/2.0/clusters/start"
SUBMIT_RUN_API = b"/api/2.0/jobs/runs/submit"
RUNS_STATUS_API = b"/api/2.0/jobs/runs/get"

# Databricks API Auth constants
# TODO: remove hardcode url
DATABRICKS_URL = "https://condenast.cloud.databricks.com/#"
RUN_TERMINATED = "TERMINATED"
RUN_SUCCESS = "SUCCESS"

# Limits and thresholds
POLLING_PERIOD_INTERVAL = config.POLLING_PERIOD_INTERVAL
WORKFLOW_THRESHOLD = 100
WORKFLOW_THRESHOLD_MIN = 5
API_CALL_TRY_COUNT = 3
INTER_API_CALL_SLEEP_TIME = 20
ERROR_PERCENT_THRESHOLD = 0.3

# General constants
EXTRACT_VENDOR_REGEX = "(?<=_)[^_]+(?=_)"

# Slackbot constants
SLACKBOT_NAME = "Spire Reporter"
SLACKBOT_AVATAR = ":report:"
SLACK_STD_OUT_CHANNEL = "datasci-spire-reports"
SLACK_STD_ERR_CHANNEL = "datasci-spire-reports"
ON_CALL = ["U0UJ0FE2V", "U02CNJN1BUL", "U01HS7K1G22"]

# Logging messages
PREPARING_TASK_LAUNCH = "preparing to launch task {}"
N_TOTAL_WORKFLOWS_SCHEDULED = "{} total workflows scheduled for stage {}"
N_WORKFLOWS_LAUNCHED_BY_VENDOR = "launching {} workflows for vendor: {}"
N_WORKFLOWS_LAUNCHED = "launching {} ready workflows"
TRAINING_SET_PREP_BY_VENDOR = (
    "preparing to assemble training sets" " for workflows for vendor: {}"
)
INSUFFICIENT_WORKFLOW_COUNT = (
    "insufficient workflows scheduled \n" "found {}, which is below threshold ({})"
)
NO_WORKFLOWS_READY = "no workflows ready to launch"
NO_WORKFLOWS_SCHEDULED = "no workflows scheduled for stage {}"


# AWS constants
AWS_RESPONSE_METADATA = "ResponseMetadata"
AWS_RESPONSE_STATUS_CODE = "HTTPStatusCode"

# HTTP constants
OK = 200

# Temporary measure to import spire version without circularity with
# spire.framework.runners.constants. What we need to do is just define
# SPIRE_DBFS_URI at a different level.
SPIRE_DBFS_URI = f"dbfs:/spire/packages/spire-{__safe_version__}" "-py3-none-any.whl"

# Features-related constants
# Domestic aleph v3
COLUMN_INDEX_DOMESTIC_PATH = "/dbfs/spire/columns_lookup.yaml"
COLUMN_LIST_DOMESTIC_PATH = "/dbfs/spire/columns_list_all.yaml"
# Global aleph v3
COLUMN_INDEX_GLOBAL_PATH = "/dbfs/spire/columns_global_lookup.yaml"
COLUMN_LIST_GLOBAL_PATH = "/dbfs/spire/columns_global_list_all.yaml"
