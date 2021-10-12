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
class CustomConfig(TargetConfig):
    """Config information for Custom Targets.
    If 'source' is provided, only 'source' data is transformed and written.
    """

    date: datetime.date

    vendor_name: str = "custom"
    source: str = None

    uri: str = constants.CUSTOM_URI
    input_format: str = "delta"

    @spark_session
    def __post_init__(self, spark=None):

        # Schema validation
        self.vendor_target_map = VendorTargetMap(
            xid=F.col("xid"),
            date=F.col("date").cast(DateType()),
            vendor=F.lit(self.vendor_name),
            source=F.col("source"),
            group=F.col("group"),
            aspect=F.to_json(F.create_map()),
            value=F.col("value").cast(FloatType()),
        )

        # Need to call super post init method to initialize query
        super().__post_init__()

    def transform(self, data: DataFrame) -> DataFrame:
        dated_data = data.filter(F.col("date") == self.date)
        # For ad hoc analysis when only transformation of a certain source
        # is required
        if self.source:
            return dated_data.filter(F.col("source") == self.source)
        return dated_data
