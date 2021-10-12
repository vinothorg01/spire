"""
TODO: Add optimize queries for partitioned tables (mcid)
After targets have finished processing, do necessary updates to the aam segments
tables.

For runtime code, see spire/tasks/execute_aam_segments_tables_updates.py
"""
import datetime
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, DateType
from spire.targets import constants
from spire.utils import spark_helpers as utils
from spire.utils.spark_helpers import spark_session
from spire.utils.logger import get_logger

logger = get_logger()


@spark_session
def load_mcid_segments_table(spark=None) -> DataFrame:
    return spark.read.format("delta").load(constants.MCID_SEGMENTS_TABLE_URI)


def get_latest_aam_file_date(mcid_segments: DataFrame) -> datetime.date:
    """Get date of latest timestamp from the last aam file drop."""

    # Obtain the date of the latest AAM file (full/iter) made available
    # Use this to update the last_processed_date of the active segments table
    # to ensure that if there is a delay in AAM data, the next time the script
    # runs, it will catch up.
    mcid_segments = mcid_segments.withColumn(
        "timestamp", F.col("timestamp").cast(TimestampType())
    )
    latest_adobe_file_date = (
        mcid_segments.withColumn(
            "ts_date",
            F.date_trunc("dd", F.from_utc_timestamp("timestamp", "EST")).cast(
                DateType()
            ),
        )
        .agg(F.max("ts_date").alias("ts_date"))
        .collect()[0][0]
    )
    logger.info(f"Latest adobe file drop date: {latest_adobe_file_date}")
    return latest_adobe_file_date


@spark_session
def update_active_segments(
    latest_adobe_file_date: datetime.date, input_format: str = "delta", spark=None
):
    """Update the last processed date for the active segments"""
    active_segments = spark.read.format(input_format).load(
        constants.ACTIVE_SEGMENTS_TABLE_URI
    )
    active_segments = active_segments.withColumn(
        "last_processed_date", F.lit(latest_adobe_file_date)
    )
    return active_segments


@spark_session
def write_active_segments_update(active_segments: DataFrame, spark=None):
    """Update the aam segments table with the latest processed date"""

    from delta.tables import DeltaTable

    segs = DeltaTable.forPath(spark, constants.ACTIVE_SEGMENTS_TABLE_URI)

    segs.alias("segs").merge(
        active_segments.alias("segs_active"),
        "segs.segment_id = segs_active.segment_id",
    ).whenMatchedUpdate(
        set={
            "segment_id": "segs.segment_id",
            "segment_name": "segs.segment_name",
            "start_date": "segs.start_date",
            "end_date": "segs.end_date",
            "last_processed_date": "segs_active.last_processed_date",
        }
    ).execute()


def check_aam_targets_write(execution_date_str: str):
    """
    Note:
    Airflow marks tasks as successful even though they may throw an exception.
    In this case, the next task begins. If the targets were not created and the
    last processed date is still updated in this task, this will lead to missing
    Adobe data for some dates. Hence, it's important to check whether the aam
    targets were written.
    Note that this check only checks whether the data inside the run_date partition
    exists. Adobe targets processing has auto-backfill so it can involve writes to
    multiple partitions. However, checking the run_date partition can serve as a proxy
    to see whether targets were processed.
    """
    execution_date = datetime.datetime.strptime(execution_date_str, "%Y-%m-%d").date()
    # The execution_date already has a 1 day lag built in
    query = f"vendor = 'adobe' AND source = 'mcid' AND date = '{execution_date}'"
    partitions_exist = utils.check_if_partitions_exist(
        constants.TARGETS_OUTPUT_URI, query
    )
    return partitions_exist
