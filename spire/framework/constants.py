from spire.config import config
from spire.version import __version__
from spire.utils.general import get_docker_image

# env
ENV = config.DEPLOYMENT_ENV
AIRFLOW_FILES_PATH = "/usr/local/airflow/include"


# cluster constants
MIN_WORKER_DEFAULT = 2
MAX_WORKER_DEFAULT = 4

# stage aliases
ASSEMBLY = "assembly"
TRAINING = "training"
SCORING = "scoring"
POSTPROCESSING = "postprocessing"

# stage run names
ASSEMBLY_RUN = "assembly-{}-run".format(ENV)
TRAINING_RUN = "training-{}-run".format(ENV)
SCORING_RUN = "scoring-{}-run".format(ENV)
POSTPROCESSING_RUN = "postprocessing-{}-run".format(ENV)

# databricks api parameter constants
RUN_TERMINATED = "TERMINATED"
CLUSTER_TYPES = ["existing_cluster_id", "new_cluster"]
TASK_TYPES = ["spark_python_task", "notebook_task"]

# general spire stage constants
STAGES = ["assembly", "training", "scoring", "postprocessing"]
DEPENDENCIES = []
WHEEL_DEPS = []

# dataset assembly constants
ASSEMBLY_CLUSTER_TYPE = "new_cluster"
ASSEMBLY_RUNNER_SCRIPT = "/{}/spire/assembly/runner".format(ENV.capitalize())
ASSEMBLY_TASK_TYPE = "notebook_task"
ASSEMBLY_TIMEOUT_SECONDS = 36000

# model training constants
TRAINING_CLUSTER_TYPE = "new_cluster"
TRAINING_RUNNER_SCRIPT = "/{}/spire/train/runner".format(ENV.capitalize())
TRAINING_TASK_TYPE = "notebook_task"
TRAINING_TIMEOUT_SECONDS = 36000

# model scoring constants
SCORING_CLUSTER_TYPE = "new_cluster"
SCORING_RUNNER_SCRIPT = "/{}/spire/score/runner".format(ENV.capitalize())
SCORING_TASK_TYPE = "notebook_task"
SCORING_TIMEOUT_SECONDS = 36000

# postprocessing score results constants
POSTPROCESSING_CLUSTER_TYPE = "new_cluster"
POSTPROCESSING_RUNNER_SCRIPT = "/{}/spire/postprocessing_in_spire/runner".format(
    ENV.capitalize()
)
POSTPROCESSING_TASK_TYPE = "spark_python_task"
POSTPROCESSING_TIMEOUT_SECONDS = 36000

# targets constants
# TODO(MAX): Check if these are used. I believe most of these constants
# are now managed in targets.constants
TARGETS_CLUSTER_TYPE = "new_cluster"
TARGETS_TASK_TYPE = "notebook_task"
TARGETS_TIMEOUT_SECONDS = 36000

AAM_TARGETS_TASK = "aam-targets-{}".format(ENV)
ACXIOM_TARGETS_TASK = "acxiom-targets-{}".format(ENV)
CDS_TARGETS_TASK = "cds-targets-{}".format(ENV)
DFP_TARGETS_TASK = "dfp-targets-{}".format(ENV)
NCS_CUSTOM_TARGETS_TASK = "ncs-custom-targets-{}".format(ENV)
POLK_TARGETS_TASK = "polk-targets-{}".format(ENV)

AAM_TARGETS_RUNNER_SCRIPT = "/{}/spire/targets/aam".format(ENV.capitalize())
ACXIOM_TARGETS_RUNNER_SCRIPT = "/{}/spire/targets/acxiom".format(ENV.capitalize())
CDS_TARGETS_RUNNER_SCRIPT = "/{}/spire/targets/cds".format(ENV.capitalize())
DFP_TARGETS_RUNNER_SCRIPT = "/{}/spire/targets/dfp".format(ENV.capitalize())
NCS_CUSTOM_TARGETS_RUNNER_SCRIPT = "/{}/spire/targets/ncs_custom".format(
    ENV.capitalize()
)
POLK_TARGETS_RUNNER_SCRIPT = "/{}/spire/targets/polk".format(ENV.capitalize())

# launch limits and thresholds
POLLING_PERIOD_INTERVAL = config.POLLING_PERIOD_INTERVAL
THREADPOOL_GROUP_SIZE = config.THREADPOOL_GROUP_SIZE
WF_THRESHOLD = config.WF_THRESHOLD
WF_THRESHOLD_MIN = config.WF_THRESHOLD_MIN
API_CALL_TRY_COUNT = config.API_CALL_TRY_COUNT
INTER_API_CALL_SLEEP_TIME = config.INTER_API_CALL_SLEEP_TIME
ERROR_PERCENT_THRESHOLD = config.ERROR_PERCENT_THRESHOLD

# logging messages
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
SKIPPING_VENDOR = "skipping assembly for vendor {}"

if config.GHCR_USER and config.GHCR_PASSWORD:
    spire_image = "spire" if "dev" not in __version__ else "spire-dev"
    DOCKER_IMAGE_CONFIG = {
        "url": get_docker_image(spire_image, __version__),
        "basic_auth": {
            "username": config.GHCR_USER,
            "password": config.GHCR_PASSWORD,
        },
    }
else:
    DOCKER_IMAGE_CONFIG = None
