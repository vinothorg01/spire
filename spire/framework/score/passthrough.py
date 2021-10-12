import datetime

import pyspark.sql.functions as F
from pyspark.sql.types import FloatType

from spire.framework.score.utils import write_scores
from spire.utils.spark_helpers import spark_session
from spire.config import config
from spire.utils.logger import get_logger


logger = get_logger()


def get_passthrough_group(workflow):
    if workflow.name == "spire_adobe_2864":
        return 17907200
    elif workflow.name == "spire_condenast_2890":
        return "covid_readers"
    elif workflow.name == "spire_condenast_3443":
        return "affiliate_commerce_clicks"
    else:
        raise Exception(
            f"Passthrough workflow {workflow.name} is not implemented for"
            "scoring at this time"
        )


@spark_session
def _score_passthrough(date, workflow, do_write=True, spark=None):
    # TODO(Max): Refactor proxy / passthrough score process altogether

    # Conde nast targets which are used by some passthrough segments
    # are processed every day at 8pm with a 2 day lag.
    # This means that on 8/20, scoring runs for 8/19 and requires the
    # targets for 8/18 to be present. However, since scoring runs every morning,
    # before the conde nast targets for 8/18 have been processed, empty dataframes were
    # written.
    # To solve this issue we have introduced a 2 day lag for passthrough
    # data such that when scoring runs for 8/19 it looks for 8/17 targets.
    # Note that this is an issue because regular workflows score over a date range
    # while passthrough is only for a given date.

    logger.info(f"Scoring date is {date}.")
    date = date - datetime.timedelta(days=2)
    logger.info(f"Passthrough scores being written for {date}.")

    group = get_passthrough_group(workflow)
    scores = _transform_passthrough(spark, date, group, workflow)
    if do_write:
        write_scores(date, workflow, scores)
    run_id = None
    experiment_id = None
    stats = None
    return run_id, experiment_id, stats


def _transform_passthrough(spark, date, group, workflow):
    if date < datetime.date(2020, 3, 12):
        filldate = datetime.date(2020, 3, 12)
    else:
        filldate = date
    scores = (
        spark.read.format("delta")
        .load(f"s3a://{config.SPIRE_ENVIRON}/targets")
        .filter(
            (F.col("vendor") == workflow.vendor)
            & (F.col("date") == filldate)
            & (F.col("group") == group)
        )
        .select("xid")
        .withColumn("score", F.lit(1.0).astype(FloatType()))
        .withColumn("decile", F.lit(1))
        .dropDuplicates(["xid"])
    )
    return scores.withColumn("date", F.lit(date)).withColumn(
        "tid", F.lit(workflow.trait.trait_id)
    )
