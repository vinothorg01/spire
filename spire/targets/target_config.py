from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from spire.targets import constants
from spire.utils.logger import get_logger
from spire.utils import spark_helpers as utils
import datetime


logger = get_logger()


class TargetConfig(ABC):
    """Abstract base class for vendor configs"""

    date: datetime.date
    vendor_name: str
    source: str

    def __post_init__(self):
        # Query to delete partitions if they exist.
        # This is common to all configs except Adobe
        # which requires a custom query
        self.query = (
            f"vendor = '{self.vendor_name}' "
            f"AND source = '{self.source}' "
            f"AND date = '{self.date}'"
        )

    @abstractmethod
    def transform(self, data: DataFrame) -> DataFrame:
        pass

    def write(self, targets: DataFrame) -> None:
        """Write targets for a single date. Supports custom queries."""

        # This conditional was added to accommodate custom targets.
        # Although the process to generate custom targets runs every day, there
        # may not always be custom targets to write, in which case the targets
        # dataframe would be empty. In that case, logging that no targets were
        # written is informative.
        if not targets.rdd.isEmpty():
            partitions = ["vendor", "source", "date", "group"]
            utils.delete_partition_if_exists(constants.TARGETS_OUTPUT_URI, self.query)
            targets.write.format("delta").mode("append").partitionBy(partitions).save(
                constants.TARGETS_OUTPUT_URI
            )
        else:
            logger.info("No targets to write")
