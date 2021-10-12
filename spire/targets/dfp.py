from spire.utils.spark_helpers import spark_session
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, FloatType, StringType
import pyspark.sql.functions as F
from dataclasses import dataclass
from spire.targets import utils
from spire.targets import constants
from .target_config import TargetConfig
from .vendor_target_map import VendorTargetMap
import datetime


@dataclass
class DFPConfig(TargetConfig):
    """Parent config class for DFP.
    Processes DFP data for a single date.
    """

    date: datetime.date
    n_days: int = 0

    vendor_name: str = "dfp"
    input_format: str = "orc"

    def __post_init__(self):
        # date must be added to dfp uri to read in correct partitions
        self.uri = self.uri + f"dt={self.date}"
        # vendor map initialized here to account for
        # custom vendor, source, aspects for DFP Clicks
        # and DFP Impressions
        self.vendor_target_map = VendorTargetMap(
            xid=F.col("custom_targeting.vnd_4d_xid"),
            date=F.lit(self.date).cast(DateType()),
            vendor=F.lit(self.vendor_name),
            source=F.lit(self.source),
            group=F.col("order_id").cast(StringType()),
            aspect=utils.create_aspect_map(self.aspect_list),
            value=F.lit(constants.DEFAULT_TARGET_VALUE).cast(FloatType()),
        )
        # Need to call super post init method to initialize query
        super().__post_init__()

    @spark_session
    def transform(
        self, data: DataFrame, order_ids: str = None, spark=None
    ) -> DataFrame:
        """Transform raw DFP clicks or impressions data.

        Loads order ids from dfp_orders table if no custom
        order id is input.

        Note
        ----
        Use DFPConfig transform for custom order ids instead of
        TargetsRunner transform.
        """
        if not order_ids:
            order_ids = utils.dfp_load_order_ids()
        return data.filter(F.col("order_id").isin(order_ids))


@dataclass
class DFPClicksConfig(DFPConfig):
    """Information for DFP clicks.

    Inherits vendor_targets_map, transform method
    and other config information from parent DFPConfig.
    """

    uri: str = constants.DFP_NETWORK_CLICKS_URI
    source: str = "network_clicks"

    @spark_session
    def __post_init__(self, spark=None):
        # aspect_list initialized here to have access to spark session
        self.aspect_list = [
            F.lit("creative_size"),
            "creative_size",
            F.lit("creative_id"),
            "creative_id",
            F.lit("line_item_id"),
            "line_item_id",
            F.lit("ad_unit_id"),
            "adunit_id",
        ]
        # Need to call super post init method to initialize vendor_target_map
        super().__post_init__()


@dataclass
class DFPImpressionsConfig(DFPConfig):
    """Information for DFP impressions.

    Inherits vendor_targets_map, transform method
    and other config information from parent DFPConfig.
    """

    uri: str = constants.DFP_NETWORK_IMPRESSIONS_URI
    source: str = "network_impressions"

    @spark_session
    def __post_init__(self, spark=None):
        # aspect_list initialized here to have access to spark session
        self.aspect_list = [
            F.lit("creative_size"),
            "creative_size",
            F.lit("creative_id"),
            "creative_id",
            F.lit("line_item_id"),
            "line_item_id",
            F.lit("referrer_url"),
            "referer_url",
            F.lit("active_view_eligible_impression"),
            "activevieweligibleimpression",
        ]
        # Need to call super post init method to initialize vendor_target_map
        super().__post_init__()
