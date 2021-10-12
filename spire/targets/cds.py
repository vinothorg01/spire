from spire.utils.spark_helpers import spark_session
from dataclasses import dataclass
from pyspark.sql.types import DateType, FloatType
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from spire.targets import utils
from spire.targets import constants
from .target_config import TargetConfig
from .vendor_target_map import VendorTargetMap
import datetime


@dataclass
class CDSConfig(TargetConfig):
    """Parent config class for CDS."""

    date: datetime.date
    n_days: int = 0

    vendor_name: str = "cds"
    input_format: str = "orc"

    def __post_init__(self):
        # vendor map initialized here to account for
        # custom vendor, source, aspect for CDS Individual
        # and CDS Individual Demo.
        self.vendor_target_map = VendorTargetMap(
            xid=F.col("xid"),
            date=F.col("date").cast(DateType()),
            vendor=F.lit(self.vendor_name),
            source=F.lit(self.source),
            group=F.col("group"),
            aspect=F.col("aspect"),
            value=F.lit(constants.DEFAULT_TARGET_VALUE).cast(FloatType()),
        )
        # Need to call super post init method to initialize query
        super().__post_init__()

    def transform(self, data: DataFrame) -> DataFrame:
        """Transform CDS individual or individual demo data.

        Joins CDS data to cds_ga to obtain xids.
        Creates group columns with {aspect: aspect_val} and converts data
        to long format.

        Note
        ----
        Previously, the joined cds ga to cds_individual and then to cds_individual_demo
        thus subsetting the cds data to only include universal ids in both.
        However, CDS indvidual and CDS individual may not have the same universal ids.
        Hence, they need to be joined to cds_ga separately to account for different
        universal ids and consequently, xids.
        We may see an uptick in the numbers of cds individual or cds_individual_demo
        due to this change.
        """
        cds_ga = utils.get_cds_data(self.date, self.n_days)
        cds_ga_join = cds_ga.join(data, "universal_id")
        transformed = utils.create_group_aspect_map(cds_ga_join, self.aspect_list)
        transformed = utils.to_long_format(transformed, "group", "aspect")
        return transformed.filter(F.col("date") == self.date).drop_duplicates()


@dataclass
class CDSIndividualConfig(CDSConfig):
    """Information for CDS Individual.

    Inherits vendor_targets_map, transform method
    and other config information from parent CDSConfig.
    """

    uri: str = constants.CDS_ALT_INDIV_URI
    source: str = "alt_individual"

    @spark_session
    def __post_init__(self, spark=None):
        # aspect_list initialized here to have access to spark session
        self.aspect_list = {"gender": [F.lit("gender"), "gender_cd"]}
        # Need to call super post init method to initialize vendor_target_map
        super().__post_init__()


@dataclass
class CDSIndividualDemoConfig(CDSConfig):
    """Information for CDS Individual Demo.

    Inherits vendor_targets_map, transform method
    and other config information from parent CDSConfig.
    """

    uri: str = constants.CDS_ALT_INDIV_DEMO_URI
    source: str = "alt_individual_demo"

    @spark_session
    def __post_init__(self, spark=None):
        # aspect_list initialized here to have access to spark session
        self.aspect_list = {
            "age": [F.lit("age"), "i1_exact_age"],
            "gender": [F.lit("gender"), "i1_gndr_code"],
            "income": [F.lit("income_class"), "experian_income_cd_v6"],
        }
        # Need to call super post init method to initialize vendor_target_map
        super().__post_init__()
