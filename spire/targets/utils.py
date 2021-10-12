"""Helper functions for use in the Spire Targets process"""
import re
import datetime
from spire.utils.spark_helpers import spark_session
from spire.integrations.postgres import connector
from pyspark.sql import functions as F
from spire.framework.workflows import DFPOrder
from typing import List, Dict, Union
from pyspark.sql.column import Column
from pyspark.sql import DataFrame
from spire.targets import constants
from spire.utils.logger import get_logger
from spire.utils import s3

logger = get_logger()


def create_aspect_map(aspect_list: List[Union[Column, str]]) -> Column:
    """Return string map for input list with column expressions grouped as key
    value pairs."""
    return F.to_json(F.create_map(aspect_list))


def create_group_aspect_map(
    data: DataFrame, aspect_dict: Dict, common_cols: List[str] = ["xid", "date"]
):
    """Select, rename and populate group columns with aspect maps.

    Args
    data: Spark Dataframe: input dataframe with common cols + group cols
    aspect_dict: Dict: {group column: [aspect name: vendor column]}
    common_cols: List: Columns to hold constant/freeze

    Returns
    data: Spark Dataframe: contains common cols and group cols with values

    Example
    aspect_dict = {group_col_1: [F.lit("aspect"), F.col("vendor_col1")],
                   group_col_2: [F.lit("aspect"), F.col("vendor_col2")]}
    create_group_aspect_map(input dataframe, aspect_dict, common_cols=['xid'])

    input dataframe                     output dataframe
    ----------------------------------- ----------------------------------------
    | xid | vendor_col1 | vendor_col2 | | xid | group_col1     | group_col2    |
    ----------------------------------- ----------------------------------------
    | A   | val1        | val2        | | A   | {aspect: val1} | {aspect: val2}|
    ----------------------------------- ----------------------------------------
    """
    group_cols = list(aspect_dict.keys())
    for group_col, aspect_map in aspect_dict.items():
        data = data.withColumn(group_col, create_aspect_map(aspect_map))
    return data.select(*[common_cols + group_cols])


def to_long_format(
    df: DataFrame, col_key: str, col_val: str, by: List[str] = ["xid", "date"]
) -> DataFrame:
    """Convert dataframe from wide to long format and name resulting
    key, value columns.

    Convert all columns that are not in the by list to long format.

    Args
    df: Spark Dataframe: input dataframe in wide format
    col_key: Str: Name for resulting key column
    col_val: Str: Name for resulting value column
    by: List: Columns to hold constant/freeze and explode

    Returns
    df: Spark Dataframe: input dataframe in long format with new columns

    Example
    to_long_format(df, "col_key", "col_val", by=["xid"])

    input dataframe                          output dataframe
    ---------------------------------------- ------------------------------
    | xid | group_col1     | group_col2    | | xid | col_key    | col_val |
    ---------------------------------------- ------------------------------
    | A   | val1           | val2          | | A   | group_col1 | val1    |
    ---------------------------------------- ------------------------------
                                             | A   | group_col2 | val2    |
                                             ------------------------------
    """
    # Filter dtypes and split into column names and type description
    cols, dtypes = zip(
        *((col, col_type) for (col, col_type) in df.dtypes if col not in by)
    )
    # Spark SQL supports only homogeneous columns
    assert len(set(dtypes)) == 1, "All columns have to be of the same type"
    # Create and explode an array of (column_name, column_value) structs
    groups_list = [
        F.struct(F.lit(col).alias(col_key), F.col(col).alias(col_val)) for col in cols
    ]
    groups = F.explode(F.array(groups_list)).alias("groups")
    return df.select(by + [groups]).select(
        by + [f"groups.{col_key}", f"groups.{col_val}"]
    )


def filter_null_xids(df: DataFrame) -> DataFrame:
    """Helper to remove all strings not conforming to UUID regex pattern from
    a passed dataframe."""
    return df.filter(F.col("xid").rlike(constants.XID_REGEX))


def get_cds_data(date: datetime.date, n_days: int = 0) -> DataFrame:
    """Wrapper function to get cds data. See _collect_all_cds_data for more details.

    Args
    date: Datetime object: Date to load ga_ids from
    n_days: Integer: Num lag days from date for ga_ids load

    Returns
    all_cds_data: Spark Dataframe: Output of _collect_all_cds_data.
                                   Data from all 3 CDS tables with
                                   universal ids mapped to XIDs.
                                   Schema = [xids, universal ids, date]
    """
    ga_ids = _load_and_filter_ga_ids(date, n_days)
    ga_xids = _get_ga_xids(ga_ids)
    return _collect_all_cds_data(ga_xids)


def dfp_load_order_ids():
    """Load all active DFP order ids being used for Spire DFP targets."""
    session = connector.make_session()
    orders = session.query(DFPOrder).filter_by(active=True).all()
    session.close()
    return [order.order_id for order in orders]


def _get_ga_files(ga_type: str) -> List[str]:
    """Get ga norm files of a particular type that have been succesfully processed."""
    ga_data_keys = s3.get_all_keys(
        constants.DATA_VENDOR_BUCKET, constants.GA_NORM_BUCKET
    )
    file_list = [
        ga_file["Key"].replace("_SUCCESS", "")
        for ga_file in ga_data_keys
        if all(keywords in ga_file["Key"] for keywords in ["_SUCCESS", ga_type])
    ]
    return file_list


def _check_ga_data_exists(
    dates_list: List[str], file_dates_dict: Dict[str, str]
) -> List[str]:
    """Check to ensure that the files exists for the dates in the date list."""
    # Intersect the GA file dates with the list of dates for n_days
    file_dates = list(file_dates_dict.keys())
    dates_found = list(set(file_dates) & set(dates_list))
    dates_not_found = list(set(dates_list) - set(dates_found))
    if len(dates_found) == 0:
        raise Exception(f"No GA data found at all for: {dates_list}")
    if len(dates_not_found) > 0:
        logger.info(
            f"GA data not found for some dates but continuing: {dates_not_found}"
        )
    else:
        logger.info("All GA data found")
    return dates_found


def get_ga_files_uri_list(dates_list: List[str], ga_type: str) -> List[str]:
    """List of ga identity table file URIs for a given date range and ga type."""
    ga_file_list = _get_ga_files(ga_type)
    # Create date: file pairs for the files. To get the dates of the files,
    # dates need to be extracted from the file paths.

    def extract_date(file_uri):
        return (re.search(r"(?<=dt=).*?(?=/)", file_uri).group(0).split("_"))[0]

    file_dates_dict = {extract_date(file_uri): file_uri for file_uri in ga_file_list}
    logger.info(f"GA type = {ga_type}")
    dates_found = _check_ga_data_exists(dates_list, file_dates_dict)
    return [
        f"s3a://{constants.DATA_VENDOR_BUCKET}/" + file_dates_dict[date]
        for date in dates_found
    ]


@spark_session
def _load_and_filter_ga_ids(
    date: datetime.date, n_days: int = 0, spark=None
) -> DataFrame:
    """Load all XIDs from ga.norm_identity from a specific date onwards.

    If no data exists for a particular date, an exception is thrown.
    See https://cnissues.atlassian.net/browse/SPIRE-320
    """
    ga_ids = spark.read.parquet(constants.GA_NORM_TABLE_URI)
    start_date = date - datetime.timedelta(days=n_days)
    ga_ids_filtered = ga_ids.filter(F.col("dt") >= start_date)
    if ga_ids_filtered.rdd.isEmpty():
        raise Exception(f"No data in GA Norm starting {start_date}")
    else:
        return ga_ids_filtered.withColumnRenamed("dt", "date")


@spark_session
def _prep_xids(ga_ids: DataFrame, spark=None) -> DataFrame:
    """Filter out valid infinity ids as xids."""
    xids = ga_ids.filter(F.col("type") == "infinity_id")
    xids = xids.withColumn("xid", F.col("mapped_id")).filter(
        F.col("xid").rlike(constants.XID_REGEX)
    )
    return xids.select("date", "xid", "fullvisitor_id")


@spark_session
def _prep_cds_ga_map_ids(ga_ids: DataFrame, spark=None) -> DataFrame:
    """Filter out GA ids to be used for mapping CDS to GA."""
    cds_ga_map_ids = ga_ids.filter(F.col("type").isin(constants.CDS_GA_MAP_IDS))
    cds_ga_map_ids = cds_ga_map_ids.withColumnRenamed("mapped_id", "id")
    return cds_ga_map_ids.select("date", "fullvisitor_id", "type", "id")


@spark_session
def _get_ga_xids(ga_ids: DataFrame, spark=None) -> DataFrame:
    """Preprocess GA XIDs from ga.norm_identity.

    Args
    ga_ids: Spark Dataframe: ga_ids from _load_and_filter_ga_ids

    Returns
    ga_xids: Spark Dataframe: Processed XIDs ready for CDS join on 'id'
    """
    xids = _prep_xids(ga_ids)
    cds_ga_map_ids = _prep_cds_ga_map_ids(ga_ids)
    ga_xids = xids.join(cds_ga_map_ids, ["date", "fullvisitor_id"])
    return ga_xids.select("xid", "type", "id", "date").distinct()


def _get_cds_ga_map():
    """Map CDS tables (cds cols) --> GA data (ga_ids)."""
    mdc_cust_map = [{"cds_col": F.col("cust_id"), "ga_id": "mdw_id"}]
    mdm_site_map = [{"cds_col": F.col("amg_uuid"), "ga_id": "amg_user_id"}]
    subs_agg_map = [
        {
            "cds_col": F.col("CDS_accountnbr_withcheckdigit"),
            "ga_id": "cds_account_number",
        }
    ]
    cds_ga_map = {
        "alt_mdc_cust": mdc_cust_map,
        "alt_mdm_site": mdm_site_map,
        "alt_subscription_aggr": subs_agg_map,
    }
    return cds_ga_map


@spark_session
def _load_cds_tables(cds_source: str, cds_col: str, spark=None) -> DataFrame:
    """Load CDS tables.

    Args:
    cds_source: String: the CDS database to load data from
    cds_col: String: col to use for joining GA with CDS

    Returns:
    cds_table: Spark Dataframe: CDS table with renamed column for joining
    """
    cds_table = spark.read.format("orc").load(constants.CDS_ALT_URI + cds_source)
    return cds_table.withColumn("id", cds_col)


@spark_session
def _join_cds_with_ga(
    ga_xids: DataFrame, ga_id_name: str, cds_table: DataFrame, spark=None
) -> DataFrame:
    """Find and joins XIDs corresponding to specific CDS table.

    Args:
    ga_xids: Spark Dataframe: Preprocessed XIDs from _get_ga_xids
    ga_id_name: String: the CDS table specific GA ID to join on
    cds_table: Spark Dataframe: CDS table/fixture from _load_cds_tables

    Returns:
    ga_cds_join: Spark Dataframe: CDS universal ids linked to GA XIDs
    """
    ga_xids_cds = ga_xids.filter(F.col("type") == F.lit(ga_id_name))
    ga_cds_join = ga_xids_cds.join(cds_table, "id")
    return ga_cds_join.select("xid", "universal_id", "date")


@spark_session
def _collect_all_cds_data(ga_xids: DataFrame, spark=None) -> DataFrame:
    """Collect data from all CDS tables with GA XIDs linked to universal ids.

    For every CDS table, loads in table, joins CDS table to GA XIDs data such that
    cds_col('X') == ga_id('id').
    Args:
    ga_xids: Spark Dataframe: XIDs from _get_ga_xids()

    Returns:
    all_cds: Spark Dataframe: Data from all 3 CDS tables with
                              universal ids mapped to XIDs.
                              Schema = [xids, universal ids, date]
    """
    all_cds = None
    cds_ga_map = _get_cds_ga_map()

    for cds_table_name, cds_ga_map_cols in cds_ga_map.items():
        for join_cols in cds_ga_map_cols:

            cds_table = _load_cds_tables(cds_table_name, join_cols["cds_col"])
            xids_with_cds = _join_cds_with_ga(ga_xids, join_cols["ga_id"], cds_table)

            if all_cds is None:
                all_cds = xids_with_cds
            else:
                all_cds = all_cds.union(xids_with_cds)

    return all_cds.distinct()
