from spire.utils.spark_helpers import spark_session
from pyspark.sql.types import DateType, FloatType
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from spire.targets import utils
from spire.targets import constants
from dataclasses import dataclass
from .target_config import TargetConfig
from .vendor_target_map import VendorTargetMap
import datetime


@dataclass
class NCSSyndicatedConfig(TargetConfig):
    """Config information for NCS Syndicated."""

    date: datetime.date
    n_days: int = 0

    vendor_name: str = "ncs"
    source: str = "syndicated"

    uri: str = constants.NCS_SYNDICATED_URI
    input_format: str = "parquet"

    @spark_session
    def __post_init__(self, spark=None):
        # aspect_list, vendor_target_map initialized here to have access to
        # spark session
        self.aspect_list = [F.lit("action"), F.lit("purchase")]
        self.vendor_target_map = VendorTargetMap(
            xid=F.col("xid"),
            date=F.col("date").cast(DateType()),
            vendor=F.lit(self.vendor_name),
            source=F.lit(self.source),
            group=F.col("group"),
            aspect=utils.create_aspect_map(self.aspect_list),
            value=F.col("value").cast(FloatType()),
        )
        # Need to call super post init method to initialize query
        super().__post_init__()

    def transform(self, data: DataFrame) -> DataFrame:
        """Transform raw NCS Syndicated data.

        NCS data is joined to cds_ga to obtain xids.
        Some xids may map to multiple universal ids
        and hence have multiple values (0 or 1).
        Taking the max ensures that if an xid has a positive
        value, it's used.
        """
        cds = utils.get_cds_data(self.date, self.n_days).select(
            "universal_id", "xid", "date"
        )
        ncs_segments = data.join(cds, "universal_id").drop("universal_id")
        ncs = utils.to_long_format(ncs_segments, "group", "value")
        ncs_grouped = ncs.groupBy("xid", "date", "group").agg(
            F.max("value").alias("value")
        )
        return ncs_grouped.filter(F.col("date") == self.date)


@dataclass
class NCSCustomConfig(TargetConfig):
    """Config information for NCS Custom."""

    date: datetime.date
    n_days: int = 0

    uri: str = constants.NCS_CUSTOM_URI
    input_format: str = "parquet"

    vendor_name: str = "ncs"
    source: str = "custom"

    @spark_session
    def __post_init__(self, spark=None):
        # vendor map initialized here to have access to spark session
        self.vendor_target_map = VendorTargetMap(
            xid=F.col("xid"),
            date=F.col("date").cast(DateType()),
            vendor=F.lit(self.vendor_name),
            source=F.lit(self.source),
            group=F.col("campaign"),
            aspect=F.to_json(F.create_map()),
            value=F.col("scale").cast(FloatType()),
        )
        # Need to call super post init method to initialize query
        super().__post_init__()

    def transform(self, data: DataFrame) -> DataFrame:
        """Transform raw NCS custom data.

        NCS data is joined to cds_ga to obtain xids.
        Some scale values are null and should be 0.
        Using the average function converts null --> 0
        and keeps 1 as it is.
        """
        cds_ga = utils.get_cds_data(self.date, self.n_days)
        ncs_ga = cds_ga.join(data, "universal_id")
        ncs_grouped = ncs_ga.groupBy("xid", "date", "campaign").agg(
            F.avg("scale").alias("scale")
        )
        return ncs_grouped.filter(F.col("date") == self.date)
