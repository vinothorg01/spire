"""
Description
-----------
This script creates and updates a table containing Adobe related mcid and
segments information.

This table will keep track of mcids and their corresponding segments based
on the source of the data (type), the date it was added (timestamp) and the
date of the most recent full file (ref_date).

Schema: (ref_date,type,timestamp,mcid,segment_array)

See https://github.com/CondeNast/spire/issues/496

Overview of steps
-----------------
Get Info files to process:
Files dropped since latest timestamp on AAM table; if AAM table does not exist,
since the recent full file.
Get list of files within a info file
Returns list of text files given a single info file
Read from given list of files and write to delta table

For runtime code, see spire/tasks/execute_mcid_segments_update.py
"""
import re
import time
import datetime
import pendulum
from typing import List, Dict, Any, Tuple, Optional
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from spire.utils import s3
from spire.targets import constants
from spire.utils.logger import get_logger
from spire.utils.spark_helpers import spark_session
from pyspark.sql.types import ArrayType, IntegerType
from spire.utils import spark_helpers as utils

logger = get_logger()


def get_aam_files(file_suffix: str) -> Dict[str, str]:
    """Get all aam file keys and create file name:file URI pairs.

    Input
    -----
    file_suffix: str: Type of AAM file to read - .sync or .info

    Output
    ------
    file_uri_dict: Dict[str,str]: {AAM file name : AAM file URI} pairs.
    """
    aam_files_list = s3.get_all_keys(
        constants.DATA_VENDOR_BUCKET, constants.AAM_MCID_SEGS_DIR
    )
    aam_file_list = [
        aam_file["Key"] for aam_file in aam_files_list if file_suffix in aam_file["Key"]
    ]
    file_uri_dict = {
        file_path.split("/")[-1]: constants.DATA_VENDOR_URI + file_path
        for file_path in aam_file_list
    }
    return file_uri_dict


def create_aam_info_file_dict(info_files: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """Extract information from aam info files into a dictionary."""
    tz = pendulum.timezone(constants.TIMEZONE)
    aam_info_file_dict = {}
    for name, uri in info_files.items():
        match = re.search(r"(full_|iter_)(\d+)", name)
        info = match.group(0).split("_")
        file_type = info[0]
        file_timestamp = int(int(info[1]) / 1000)
        aam_info_file_dict[name] = {
            "type": file_type,
            "timestamp": file_timestamp,
            "date": datetime.datetime.fromtimestamp(file_timestamp, tz).date(),
            "uri": uri,
        }
    return aam_info_file_dict


@spark_session
def load_mcid_segments(input_format="delta", spark=None) -> Optional[DataFrame]:
    try:
        mcid_segments = spark.read.format(input_format).load(
            constants.MCID_SEGMENTS_TABLE_URI
        )
    except Exception:
        mcid_segments = None
        logger.info("AAM mcid segments table not found, creating one...")
    return mcid_segments


def get_latest_mcid_segments_timestamp(
    mcid_segments: DataFrame, aam_info_file_dict: Dict[str, Dict[str, Any]]
) -> int:
    """Extract latest timestamp from aam table or latest full file.
    Files available after this timestamp will be processed.
    """
    if mcid_segments is not None:
        latest_mcid_segments_timestamp = mcid_segments.agg(
            F.max("timestamp")
        ).collect()[0][0]
    else:
        # If the AAM table does not exist, start processing from the date of the
        # last full file
        latest_full_file_timestamp = max(
            [
                file_info["timestamp"]
                for file_name, file_info in aam_info_file_dict.items()
                if file_info["type"] == "full"
            ]
        )
        # - 3600 to make sure that the full file is read in when
        # filter_info_files_by_timestamp is called
        latest_mcid_segments_timestamp = latest_full_file_timestamp - 3600
    return latest_mcid_segments_timestamp


def filter_info_files_by_timestamp(
    aam_info_file_dict: Dict[str, Dict[str, Any]], latest_mcid_segments_timestamp: int
) -> List[str]:
    """Returns list of info files received after the latest timestamp in AAM table.

    If AAM table does not exist, since the recent full file from logic in
    get_latest_mcid_segments_timestamp.
    """
    info_files_to_process = [
        [file_name, file_info["timestamp"]]
        for file_name, file_info in aam_info_file_dict.items()
        if latest_mcid_segments_timestamp < file_info["timestamp"]
    ]
    # Sort files based on timestamp to make sure that the files added first get
    # processed first so that updates are made in a sequential manner
    info_files_to_process.sort(key=lambda value: value[1])
    return [file_name for file_name, timestamp in info_files_to_process]


@spark_session
def get_aam_sync_file_names_list(aam_info_file_uri: str, spark=None) -> List[str]:
    """Returns list of sync file names in an info file."""
    df = spark.read.option("multiLine", "True").json(aam_info_file_uri)
    return df.select("Files.Filename").collect()[0][0]


def get_sync_files_uri_list(sync_file_names: List[str]) -> List[str]:
    """Returns list of URIs for the sync files listed in the info file.
    If the sync files cannot be found, raises an exception.
    """
    sync_files = get_aam_files(".sync")
    try:
        return [sync_files[str(file_name)] for file_name in sync_file_names]
    except KeyError as e:
        logger.error(
            f"A sync file listed in the info file was not found: {e.args[0]}",
            exc_info=True,
        )
        raise


@spark_session
def load_aam_segments_data(
    sync_file_names: List[str],
    max_tries: int = 10,
    wait_time_per_check: int = 120,
    spark=None,
) -> DataFrame:
    """
    Loads in mcids and their corresponding segments from the sync files
    listed in the info file.

    Note
    ----
    This function also handles the edge case wherein a file transfer is occurring
    concurrently by waiting for the transfer to complete. This is done implicitly
    by creating a list of the sync file URIs and trying to load them.
    If any file is not found at the location obtained from the s3 bucket, it can be
    assumed that a transfer is occurring as the location has been altered.
    The process waits for 2 minutes and then tries looking for the locations of the
    sync files again. The process repeats until all files are found and read
    successfully or 20 minutes have elapsed.
    """
    for tries in range(max_tries):
        logger.info("Fetching sync file URIs...")
        sync_files_uri_list = get_sync_files_uri_list(sync_file_names)
        try:
            data = spark.read.csv(sync_files_uri_list)
            logger.info("All files found!")
            break
        except Exception:
            logger.error("A file was not found. Trying again in 2 mins...")
            time.sleep(wait_time_per_check)
        if tries == (max_tries - 1):
            raise Exception(
                f"Failed {max_tries} times. Try again after files are transferred."
            )
    return data


def parse_aam_segments(aam_segs_raw: DataFrame) -> DataFrame:
    """Extract mcid and segments from raw data.

    Args
    aam_segs_raw: DataFrame: Unparsed aam segments data

    Returns
    aam_segs: Dataframe: Parsed mcid and list of corresponding aam segments
    """
    aam_segs = aam_segs_raw.withColumn("array", F.split("_c0", " "))
    aam_segs = aam_segs.filter(F.size(F.col("array")) > 1)
    aam_segs = aam_segs.withColumn("mcid", F.col("array").getItem(0))
    aam_segs = aam_segs.filter(F.col("mcid").rlike(constants.MCID_REGEX)).drop("_c0")
    aam_segs = aam_segs.withColumn(
        "segment_array",
        F.expr("slice(array,2,size(array)-1)").cast(ArrayType(IntegerType())),
    )
    return aam_segs.select("mcid", "segment_array")


def get_latest_ref_date(
    info_file_dict: Dict[str, Any], mcid_segments: DataFrame
) -> Dict[str, Any]:
    """
    Add ref_date to the info dict.

    Note
    ----
    ref_date corresponds to the date of the latest full file
    that is to be used as a reference date.

    If iter files are being processed, the ref_date is obtained from
    the mcid_segments. For full files, the date extracted from their timestamp
    is used as the ref_date.
    """
    if info_file_dict["type"] == "iter":
        latest_ref_date = (
            mcid_segments.filter(F.col("ref_date") <= F.lit(info_file_dict["date"]))
            .agg(F.max("ref_date"))
            .collect()[0][0]
        )
    else:
        latest_ref_date = info_file_dict["date"]
    info_file_dict["ref_date"] = latest_ref_date
    return info_file_dict


def update_mcid_segments(
    info_file_dict: Dict[str, Any], parsed: DataFrame
) -> DataFrame:
    """
    Create update to aam table.
    """
    df = parsed.select(
        F.lit(info_file_dict["ref_date"]).alias("ref_date"),
        F.lit(info_file_dict["type"]).alias("type"),
        F.lit(info_file_dict["timestamp"]).alias("timestamp"),
        "mcid",
        "segment_array",
    )
    return df


def get_info_files_to_process(
    mcid_segments: DataFrame,
) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
    """
    Wrapper function to extract info files and their attributes.
    """
    # Extract list of info files and create dictionary of relevant attributes
    info_files = get_aam_files(".info")
    aam_info_file_dict = create_aam_info_file_dict(info_files)
    # Get info files created after last timestamp
    latest_mcid_segments_timestamp = get_latest_mcid_segments_timestamp(
        mcid_segments, aam_info_file_dict
    )
    logger.info("Sorting info files by timestamp...")
    info_files_to_process = filter_info_files_by_timestamp(
        aam_info_file_dict, latest_mcid_segments_timestamp
    )
    return info_files_to_process, aam_info_file_dict


def load_and_process_aam_segments(info_file_dict: Dict[str, Any]) -> DataFrame:
    """
    Wrapper function to process aam segments from sync files.
    """
    sync_files_list = get_aam_sync_file_names_list(info_file_dict["uri"])
    raw_aam_segments = load_aam_segments_data(sync_files_list)
    logger.info("Parsing sync files...")
    parsed_aam_segments = parse_aam_segments(raw_aam_segments)
    return update_mcid_segments(info_file_dict, parsed_aam_segments)


def write_mcid_segments(
    mcid_segments_update: DataFrame, info_file_dict: Dict[str, Any]
) -> None:
    """
    Output the updates made to the mcids and segments in the aam table.
    """
    query = (
        f"ref_date='{info_file_dict['ref_date']}' "
        f"AND type='{info_file_dict['type']}' "
        f"AND timestamp='{info_file_dict['timestamp']}'"
    )
    utils.delete_partition_if_exists(constants.MCID_SEGMENTS_TABLE_URI, query)
    mcid_segments_update.write.format("delta").mode("append").partitionBy(
        "ref_date", "type", "timestamp"
    ).save(constants.MCID_SEGMENTS_TABLE_URI)
