from pyspark.sql import SparkSession
from sqlalchemy.orm import Session
from datetime import date
from typing import List
from spire.framework.workflows import workflow as w
from spire.utils.spark_helpers import init_spark
from spire.utils.logger import get_logger


logger = get_logger()


def postprocess_all(
    workflows: List["w.Workflow"],
    date: date,
    session: Session,
    spark: SparkSession = None,
):
    if not spark:
        spark = init_spark()
    responses = []

    for workflow in workflows:
        logger.info(f"Start processing {workflow} on {date}")
        for postprocessor in workflow.postprocessor:
            logger.info(f"\tRunning {postprocessor}")
            response = postprocessor.postprocess(date, session, spark)
            responses.append(response)

    return responses
