"""
Load active_segments.csv (need to add a refresh process that will refresh the csv file
with the latest contents of the google sheet) into a Delta table that we will call
Active Segments table.
Schema = (segment_id,segment_name,start_date,end_date,last_processed_date)
See https://github.com/CondeNast/spire/issues/496

For runtime code, see spire/tasks/execute_active_segments_update.py
"""
import json
import datetime
import pandas as pd
from typing import Optional
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from spire.utils.spark_helpers import spark_session
from pyspark.sql.types import IntegerType, DateType
from spire.utils.logger import get_logger
from spire.targets import constants
from spire.utils import spark_helpers as utils

logger = get_logger()


@spark_session
def load_aam_segments_table(
    input_format: str = "delta", spark=None
) -> Optional[DataFrame]:
    try:
        aam_segments_table = spark.read.format(input_format).load(
            constants.ACTIVE_SEGMENTS_TABLE_URI
        )
    except Exception:
        aam_segments_table = None
    return aam_segments_table


@spark_session
def extract_spire_aam_segments(execution_date: datetime.date, spark=None) -> DataFrame:
    """Load and extract list of active Adobe segments and their names for Spire."""

    # Extract spire segments into json
    spire_segs = spark.read.csv(constants.SPIRE_AAM_SEGMENTS_URI).toDF("segs")
    segs_json = json.loads(spire_segs.collect()[0]["segs"])
    # Dataframe
    segs_df = spark.createDataFrame(pd.DataFrame(segs_json))
    active_segs = segs_df.select(
        F.col("SID").cast(IntegerType()).alias("segment_id"),
        F.col("Name").alias("segment_name"),
        F.lit(execution_date).alias("start_date"),
        F.lit(None).cast(DateType()).alias("end_date"),
        F.lit(None).cast(DateType()).alias("last_processed_date"),
    )
    return active_segs


@spark_session
def get_latest_aam_segments(
    targets_format="delta",
    spark=None,
) -> Optional[DataFrame]:
    """Get list of latest segments and the date they were last processed from
    the adobe targets table. The latest segments are those that were processed
    after the latest full file."""

    logger.info("Fetching latest active segments from the targets table...")
    # Get latest full file date
    aam_table = spark.read.format(targets_format).load(
        constants.MCID_SEGMENTS_TABLE_URI
    )
    latest_ref_date = aam_table.agg(F.max("ref_date")).collect()[0][0]

    # Ensure that targets exist for dates greater than the latest ref date
    query = f"vendor='adobe' AND source='mcid' AND date >='{latest_ref_date}'"
    if utils.check_if_partitions_exist(constants.TARGETS_OUTPUT_URI, query):
        latest_aam_segments = (
            spark.read.format(targets_format)
            .load(constants.TARGETS_OUTPUT_URI)
            .filter(F.col("vendor") == "adobe")
            .filter(F.col("date") >= F.lit(latest_ref_date))
            .select("group", "date")
        )
        return latest_aam_segments
    else:
        raise Exception(f"No targets greater than {latest_ref_date} found.")


@spark_session
def create_aam_segments_table(active_aam_segments: DataFrame, spark=None) -> DataFrame:
    """Create aam segments table if it does not exist."""

    # Get latest active aam segments
    latest_aam_segments = get_latest_aam_segments()
    aam_segments = latest_aam_segments.join(
        active_aam_segments, F.col("group").cast(IntegerType()) == F.col("segment_id")
    )
    aam_segments = aam_segments.groupBy("segment_id").agg(
        F.max("date").alias("latest_date")
    )

    # Create spire aam segments table
    active_table = active_aam_segments.join(
        aam_segments, "segment_id", "left_outer"
    ).select(
        "segment_id",
        "segment_name",
        F.least(F.col("start_date"), F.col("latest_date")).alias("start_date"),
        "end_date",
        F.col("latest_date").alias("last_processed_date"),
    )

    return active_table


def update_aam_segments(
    segs_table: DataFrame, active_segs: DataFrame, execution_date: datetime.date
) -> DataFrame:
    """Returns of aam segments with updated end dates where applicable."""

    # Filter the delta table to segments without end dates to check if they are
    # still active
    segs_table = segs_table.filter(F.col("end_date").isNull())

    # If there are segments that exist in the segments delta table but not
    # in the list of active segments, they are considered inactive.
    # Add an end date to the inactive segments
    updates = segs_table.join(active_segs, on="segment_id", how="left_anti")
    updates = updates.select(
        "segment_id",
        "segment_name",
        "start_date",
        F.lit(execution_date - datetime.timedelta(days=1)).alias("end_date"),
        "last_processed_date",
    )
    return active_segs.union(updates)


@spark_session
def write_aam_segments_updates(active_segs: DataFrame, spark=None) -> None:
    """Updates the end dates of inactive segments and adds new segments."""

    # Import here as the delta functionality is available inside the
    # spark session
    from delta.tables import DeltaTable

    segsDT = DeltaTable.forPath(spark, constants.ACTIVE_SEGMENTS_TABLE_URI)
    segsDT.alias("table").merge(
        active_segs.alias("active_segs"),
        "table.segment_id = active_segs.segment_id and table.end_date is null",
    ).whenMatchedUpdate(
        set={
            "segment_id": "table.segment_id",
            "segment_name": "active_segs.segment_name",
            "start_date": "table.start_date",
            "end_date": "active_segs.end_date",
            "last_processed_date": "table.last_processed_date",
        }
    ).whenNotMatchedInsert(
        values={
            "segment_id": "active_segs.segment_id",
            "segment_name": "active_segs.segment_name",
            "start_date": "active_segs.start_date",
            "end_date": "active_segs.end_date",
            "last_processed_date": "active_segs.last_processed_date",
        }
    ).execute()
