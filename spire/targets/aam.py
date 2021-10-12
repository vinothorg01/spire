import datetime
from pyspark.sql import Window
from pyspark.sql.types import DateType, FloatType, StringType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Row
from typing import Tuple
from dataclasses import dataclass
from spire.targets import constants, utils
from .target_config import TargetConfig
from .vendor_target_map import VendorTargetMap
from spire.utils.spark_helpers import spark_session
from spire.utils.logger import get_logger

#####################################
# Adobe processing helper functions #
#####################################

logger = get_logger()


def get_latest_full_file_date(mcid_segments: DataFrame) -> datetime.date:
    """Get date of last full file processed."""
    latest_full_file_date = mcid_segments.agg(F.max("ref_date")).collect()[0][0]
    return latest_full_file_date


@spark_session
def get_active_segments(
    latest_full_file_date: datetime.date,
    execution_date: datetime.date,
    input_format: str = "delta",
    spark=None,
) -> DataFrame:
    """Get active spire segments.

    Get segments that were created before or on the execution date and that are
    currently active, i.e., they have no end date, or their end date is after the
    execution date.

    If there are active segments, update their last processed date to the execution
    date.
    Note: input_format is a parameter to allow for the test to read in a fixture
    """
    segs_table = spark.read.format(input_format).load(
        constants.ACTIVE_SEGMENTS_TABLE_URI
    )
    active_segments = segs_table.filter(
        (F.col("start_date") <= execution_date)
        & ((F.col("end_date").isNull()) | (F.col("end_date") >= execution_date))
    )
    if active_segments.rdd.isEmpty():
        raise Exception("No active segments found in date range")
    return get_processing_start_date(active_segments, latest_full_file_date)


def get_processing_start_date(
    active_segments: DataFrame, latest_full_file_date: datetime.date
) -> DataFrame:
    """Create the processing_start_date column which indicates which date to process
    a segment from.

    If the segment was never processed, i.e. it is a newly added segment,
    set its processing start date to the date of the last full file to
    ensure sufficient training data.
    Otherwise set it to whichever to whichever happened more recently -
    the last full file drop or the last processing.
    """
    segs_start_date = active_segments.withColumn(
        "processing_start_date",
        F.when(
            F.col("last_processed_date").isNull(), F.lit(latest_full_file_date)
        ).otherwise(
            F.greatest(
                F.lit(latest_full_file_date),
                F.date_add(F.col("last_processed_date"), 1),
            )
        ),
    )
    return segs_start_date


@spark_session
def get_segments_to_process(
    active_segments: DataFrame,
    latest_full_file_date: datetime.date,
    execution_date: datetime.date,
    backfill_days: int,
    spark=None,
) -> DataFrame:
    """Get list of segments to process by date.

    Note: The backfill_days parameter marks the maximum days to backfill for.
    It is currently set to 35 days in the AdobeConfig to account for any delays
    since the last full file but it may have to be revisited for any changes in
    the Adobe file drop policies.
    """
    # Group segments by the date they must be processed from.
    segments_by_dates = active_segments.groupBy("processing_start_date").agg(
        F.collect_set(F.col("segment_id")).alias("select_segs")
    )
    min_processing_start_date = segments_by_dates.agg(
        F.min("processing_start_date")
    ).collect()[0][0]
    logger.info(f"Min processing start date: {min_processing_start_date}")
    # Get a range of dates in the range (min_processing_start_date, execution_date)
    # inclusive. Max number of dates in range is specified by the num of days to
    # backfill for. Hence, this takes care of any backfills that may be required.
    dates = [
        {"processing_start_date": (latest_full_file_date + datetime.timedelta(days=i))}
        for i in range(backfill_days + 1)
        if ((latest_full_file_date + datetime.timedelta(days=i)) <= execution_date)
        & (
            (latest_full_file_date + datetime.timedelta(days=i))
            >= min_processing_start_date
        )
    ]
    if len(dates) == 0:
        raise Exception(
            "No dates to process as min processing start date is"
            + f"{min_processing_start_date} and execution date is {execution_date}"
        )
    dates_to_process = spark.createDataFrame(Row(**date) for date in dates)

    # Get list of segments to process for each date in the date range
    # For dates that were not in the segments table, the segments column
    # will be null
    segments_to_process = segments_by_dates.join(
        dates_to_process, "processing_start_date", "right_outer"
    )
    return segments_to_process


def get_segments_to_process_by_date(segs_grouped_by_dates: DataFrame) -> DataFrame:
    """
    For each start date, get the list of segments to process recursively.
    Example:
    date | segments   --> date | segments
    ---- | -------        ---- | -------
    1st  | [A, B]         1st  | [A, B]
    2nd  | [S, D]         2nd  | [A, B, S, D]
    3rd  | null           3rd  | [A, B, S, D]
    """
    window = Window.orderBy(F.col("processing_start_date")).rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    # Update the segments to process for each processing start date.
    # This ensures that segments are added to the start dates with empty segment lists
    segs_to_process = segs_grouped_by_dates.select(
        "processing_start_date",
        F.flatten(F.collect_set("select_segs").over(window)).alias("segs_to_process"),
    )
    return segs_to_process


def get_dates_and_segments(
    active_segments: DataFrame,
    latest_full_file_date: datetime.date,
    execution_date: datetime.date,
    backfill_days: int,
) -> DataFrame:
    """Wrapper function that gets active segments and creates a dataframe containing
    dates and the segments to process for that date."""

    segs_grouped_by_dates = get_segments_to_process(
        active_segments, latest_full_file_date, execution_date, backfill_days
    )
    return get_segments_to_process_by_date(segs_grouped_by_dates)


def intersect_spire_aam_segments(
    latest_full_file_date: datetime.date,
    segs_to_process: DataFrame,
    mcid_segments: DataFrame,
) -> DataFrame:
    """Subset spire segments from all segments for mcid.

    Args
    latest_full_file_date: datetime.date: latest date of full file to filter data by
    segs_to_process: Spark Dataframe: Parsed mcid and list of corresponding aam segments
    mcid_segments: Spark Dataframe: mcids and their segments for spire

    Returns
    segs: Spark Dataframe: mcids and non empty list of corresponding aam-spire segments
                           by their start dates
    """
    mcid_segments_subset = mcid_segments.filter(
        F.col("ref_date") >= latest_full_file_date
    )
    mcid_segments_subset = mcid_segments_subset.withColumn(
        "timestamp", F.col("timestamp").cast(TimestampType())
    )
    mcid_segments_subset = mcid_segments_subset.withColumn(
        "timestamp_date",
        F.date_trunc("dd", F.from_utc_timestamp("timestamp", constants.TIMEZONE)).cast(
            DateType()
        ),
    )
    segs = mcid_segments_subset.join(
        segs_to_process, F.col("timestamp_date") == F.col("processing_start_date")
    )
    # Intersect spire segments from mcid_segments with segments to process for each row.
    segs = segs.withColumn(
        "segment_select", F.array_intersect("segs_to_process", "segment_array")
    )
    intersect_segs = segs.select(
        "processing_start_date", "mcid", "segment_select"
    ).filter(F.size(F.col("segment_select")) > 0)
    return intersect_segs


@spark_session
def load_ga_mapping_tables(
    date: datetime.date, n_days: int, spark=None
) -> Tuple[DataFrame, DataFrame]:
    """Load ga tables for a given date range.

    Args
    date: datetime.date: current date
    n_days: int: days of lag for date range

    Returns
    mcid: Spark Dataframe: mcids from ga identity for a date range and pattern
    xid: Spark Dataframe: xid from ga identity for a date range and pattern
    """
    dates_list = [
        (date - datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n_days)
    ]
    mcid = (
        spark.read.format("parquet")
        .load(
            utils.get_ga_files_uri_list(
                dates_list,
                "adobe_mcid",
            )
        )
        .withColumnRenamed("mapped_id", "mcid")
        .filter(F.col("mcid").rlike(constants.MCID_REGEX))
        .distinct()
    )
    xid = (
        spark.read.format("parquet")
        .load(
            utils.get_ga_files_uri_list(
                dates_list,
                "infinity_id",
            )
        )
        .withColumnRenamed("mapped_id", "xid")
        .filter(F.col("xid").rlike(constants.XID_REGEX))
        .distinct()
    )
    return mcid, xid


def join_xid_to_segments(
    intersect_segs: DataFrame, mcid: DataFrame, xid: DataFrame
) -> DataFrame:
    """Get xid: mcid - fullvisitor_id - xid and distinct segments"""
    joined = intersect_segs.join(mcid, "mcid").join(xid, "fullvisitor_id")
    segs = joined.groupBy("processing_start_date", "xid").agg(
        F.collect_set("segment_select").alias("segment_select")
    )
    return segs.withColumn(
        "segment_select", F.array_distinct(F.flatten(F.col("segment_select")))
    )


def create_targets(
    segments_xid_join: DataFrame,
    active_segs: DataFrame,
) -> DataFrame:
    """Explode segments list to obtain groups and corresponding names."""
    adobe_targets = segments_xid_join.withColumn("group", F.explode("segment_select"))
    segs_info = active_segs.select(
        F.col("segment_id").alias("group"), "segment_name"
    ).distinct()
    adobe_targets = adobe_targets.join(segs_info, "group")
    # Obtain xid, date, group, segment_names columns
    return adobe_targets.select("xid", "processing_start_date", "group", "segment_name")


def create_query(aam_segments: DataFrame) -> str:
    """Collect the dates and groups that will be updated in this processing as a str."""

    def add_quotes(s):
        return "'" + s + "'"

    segs_list = [
        f"(date='{row.processing_start_date}' and group in "
        + f"({','.join([add_quotes(str(segment)) for segment in row.segs_to_process])}))"  # noqa
        for row in aam_segments.collect()
    ]
    replace_str = (
        "vendor='adobe' and source='mcid' and (" + " or ".join(segs_list) + ")"
    )
    return replace_str


@dataclass
class AdobeConfig(TargetConfig):
    """Config information for Adobe."""

    date: datetime.date
    n_days: int = 120  # Num days of GA data to load

    vendor_name: str = "adobe"
    source: str = "mcid"

    uri: str = constants.MCID_SEGMENTS_TABLE_URI
    input_format: str = "delta"

    backfill_days: int = 35

    @spark_session
    def __post_init__(self, spark=None):
        # aspect_list, vendor_target_map initialized here to have access to
        # spark session
        self.aspect_list = [F.lit("name"), F.col("segment_name")]
        self.vendor_target_map = VendorTargetMap(
            xid=F.col("xid"),
            date=F.col("processing_start_date"),
            vendor=F.lit(self.vendor_name),
            source=F.lit(self.source),
            group=F.col("group").cast(StringType()),
            aspect=utils.create_aspect_map(self.aspect_list),
            value=F.lit(constants.DEFAULT_TARGET_VALUE).cast(FloatType()),
        )
        # Note: super post init method is not called here since custom query that
        # is created inside the transform method.

    def transform(self, mcid_segments_table: DataFrame) -> DataFrame:
        """Transform Adobe data using private helpers."""
        latest_full_file_date = get_latest_full_file_date(mcid_segments_table)
        logger.info(f"Latest full file date: {latest_full_file_date}")

        # If it has been more than 35 days since a full file has been loaded
        # wait for it to be loaded
        if (self.date - latest_full_file_date).days > self.backfill_days:
            raise Exception("Wait for Adobe data to be loaded")

        # Get segments to process
        active_segments = get_active_segments(latest_full_file_date, self.date)

        segments = get_dates_and_segments(
            active_segments, latest_full_file_date, self.date, self.backfill_days
        )

        # Create string of dates and segments to process for the write method
        self.query = create_query(segments)

        # Create targets
        mcid, xid = load_ga_mapping_tables(self.date, self.n_days)
        intersect_segs = intersect_spire_aam_segments(
            latest_full_file_date, segments, mcid_segments_table
        )
        segments_xid_join = join_xid_to_segments(intersect_segs, mcid, xid)
        return create_targets(segments_xid_join, active_segments)
