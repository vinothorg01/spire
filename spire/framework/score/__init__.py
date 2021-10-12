import time
import datetime
import traceback

from spire.framework.score.scoring import _score
from spire.framework.score.utils import make_scoring_response
from spire.framework.execution import JobStatuses
from spire.utils.logger import get_logger
from spire.utils.spark_helpers import spark_session

logger = get_logger(__name__)


@spark_session
def score(workflow, date, session, spark=None):
    """
    This function produces trait_scores for the trait specified in workflows
    for all users active on the specified date from the features found in
    the dataframe features and writes the result out to a location specified
    in constants.
    ARGS:
        spark : spark.sql.SparkSession
            The spark context.
        workflow : spire.framework.workflow.Workflow:
            The workflow for which to score.
        date: date: the date for which to scores users.
        session : sqlalchemy session
            The session the workflow belongs to.
    RETURNS:
        response: dictionary: a loggable response for the run.
    """
    try:
        logger.info(f"Start scoring workflow {workflow}")
        run_id, experiment_id, stats = _score(workflow, date, session, spark=spark)
        status = JobStatuses.SUCCESS
        response = make_scoring_response(
            workflow,
            date,
            status,
            run_id=run_id,
            experiment_id=experiment_id,
            execution_time=datetime.datetime.fromtimestamp(time.time()),
            stats=stats,
            warnings=None,
            error=None,
            tb=None,
        )
        logger.info(f"Sucessully score workflow {workflow}", extra=response)
    except Exception as e:
        # TODO(Max): Rather than one try/except with if statements for different
        # exception types, refactor to handle this in a more dynamic way.
        tb = traceback.format_exc()
        if "MLFlowDependenceError" in tb:
            status = JobStatuses.MLFLOW_DEPENDENCE_ERROR
        elif "MissingFeaturesError" in tb:
            status = str(JobStatuses.MISSING_FEATURES_ERROR)
            status = JobStatuses.MISSING_FEATURES_ERROR
        elif "MissingHistoryError" in tb:
            status = str(JobStatuses.MISSING_HISTORY_ERROR)
            status = JobStatuses.MISSING_HISTORY_ERROR
        else:
            status = JobStatuses.FAILED
        response = make_scoring_response(
            workflow,
            date,
            status,
            run_id=None,
            experiment_id=None,
            execution_time=datetime.datetime.fromtimestamp(time.time()),
            stats=None,
            warnings=None,
            error=e,
            tb=tb,
        )
        logger.error(
            f"Failed to score workflow {workflow}", exc_info=True, extra=response
        )
    workflow.commit_history(response, session)
    return response


@spark_session
def score_all(workflows, date, session, spark=None):
    responses = []
    for workflow in workflows:
        response = score(workflow, date, session, spark=spark)
        responses.append(response)
    return responses


__all__ = [score, score_all]
