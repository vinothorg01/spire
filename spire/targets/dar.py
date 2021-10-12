from pyspark.sql.types import DateType, FloatType
from spire.utils.spark_helpers import spark_session
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from spire.targets import constants
from dataclasses import dataclass
from .target_config import TargetConfig
from .vendor_target_map import VendorTargetMap
import datetime


@dataclass
class DARConfig(TargetConfig):
    """Config information for Nielsen."""

    date: datetime.date
    n_days: int = 0

    vendor_name: str = "nielsen"
    source: str = "dar"

    uri: str = constants.DAR_URI
    input_format: str = "delta"

    @spark_session
    def __post_init__(self, spark=None):
        # vendor map initialized here to have access to spark session
        self.vendor_target_map = VendorTargetMap(
            xid=F.col("xid"),
            date=F.col("date").cast(DateType()),
            vendor=F.lit(self.vendor_name),
            source=F.lit(self.source),
            group=F.col("group"),
            aspect=F.to_json(F.create_map()),
            value=F.col("value").cast(FloatType()),
        )
        # Need to call super post init method to initialize query
        super().__post_init__()

    def transform(self, data: DataFrame) -> DataFrame:
        """No data is transformed for Nielsen."""
        return data
