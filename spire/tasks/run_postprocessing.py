from spire.framework.workflows.workflow import WorkflowStages
from spire.utils import get_logger
from .run_stage import run_stage

logger = get_logger(__name__)


def run_postprocessing(run_date):
    return run_stage(WorkflowStages.POSTPROCESSING, run_date)
