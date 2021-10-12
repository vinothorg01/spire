import datetime
import re
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from sqlalchemy.orm.session import Session
from spire.framework.score import constants
from spire.framework.workflows.workflow_stages import WorkflowStages
from spire.framework.workflows.history import History
from spire.utils.spark_helpers import delete_partition_if_exists
from spire.utils.logger import get_logger

if TYPE_CHECKING:
    # TODO(Max): Cyclic import resolution
    from spire.framework.workflows import Workflow


logger = get_logger()


def get_history(
    workflow: "Workflow", date: datetime.date, session: Session
) -> "History":
    """
    Uses the get_by_attrs History method to query the most recent training history
    for a given workflow on a given date
    """
    return History.get_last_successful_train(
        session,
        workflow_id=workflow.id,
        arg_date=date,
        stage=WorkflowStages.TRAINING,
        status="success",
    )


def get_trait(workflow):
    if "iqvia" in workflow.name:
        return int(re.search(r"spire_iqvia_(\d*)", workflow.name).group(1))
    return workflow.trait.trait_id


def write_scores(date: datetime.date, workflow: "Workflow", scores: DataFrame):
    logger.info(f"Writing parquet score to {constants.PARQUET_OUTPUT_URI}")
    parquet_path = (
        f"{constants.PARQUET_OUTPUT_URI}/date={date}/tid={workflow.trait.trait_id}/"
    )
    scores.write.format("parquet").mode("overwrite").save(parquet_path)

    query = f"date = {date.strftime('%Y-%m-%d')} AND tid = {workflow.trait.trait_id}"
    delete_partition_if_exists(constants.DELTA_OUTPUT_URI, query)

    logger.info(f"Writing delta score to {constants.DELTA_OUTPUT_URI}")
    scores.write.partitionBy("date", "tid").format("delta").mode("append").save(
        constants.DELTA_OUTPUT_URI
    )


def make_scoring_response(
    workflow,
    date,
    status,
    run_id=None,
    experiment_id=None,
    execution_time=None,
    stats=None,
    warnings=None,
    error=None,
    tb=None,
):
    """
    Helper function to produce the response object that gets logged after
    a run.
    ARGS:
        workflow : spire.framework.workflow.Workflow
            workflow that has been scored.
        date : datetime.date
            date that has been scored.
        status : string
            the status of the run (success/failure).
        run_id : string
            run_id of the model used for scoring.
        experiment_id : string
            experiment_id of the model used for scoring.
        execution_time : timestamp
            time that the model was run
        stats : dictionary
            various stats associated with scoring, currently
            this is rudimentary statistics associated with the
            output deciles.
        error : exception
            the error encountered by the run if it failed.
        tb : string
            the traceback of the error encountered by the run if it
            failed.
        warnings : dictionary
            any warnings raised by the process.
    RETURNS:
        response: dictionary: a logable response for the run.
    """
    warnings = warnings or {}
    stats = stats or {}
    info = {}
    if error is not None:
        err = str(error).replace("'", "''")
        tb = tb.replace("'", "''")
    else:
        err = None
    info["name"] = re.sub(r"(\W)", "", workflow.name)
    info["description"] = re.sub(r"(\W)", "", workflow.description)
    info["mlflow_run_id"] = run_id
    info["mlflow_exp_id"] = experiment_id

    arg_date = datetime.datetime.combine(date, datetime.datetime.min.time()).date()
    response = {
        "workflow_id": str(workflow.id),
        "stage": "scoring",
        "status": status,
        "error": err,
        "traceback": tb,
        "arg_date": arg_date,
        "execution_date": execution_time,
        "stats": stats,
        "warnings": warnings,
        "info": info,
    }
    return response
