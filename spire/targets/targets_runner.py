from spire.targets import utils
from spire.utils.spark_helpers import spark_session


class TargetsRunner:
    """Runner class to preprocess targets from various vendors"""

    def __init__(self, vendor_config):
        self.vendor_config = vendor_config

    @spark_session
    def load(self, spark=None):
        """Load vendor data according to instructions in config."""
        return spark.read.format(self.vendor_config.input_format).load(
            self.vendor_config.uri
        )

    def transform(self, data):
        """Transform vendor data according to instructions in config."""
        return self.vendor_config.transform(data)

    def map_schema(self, vendor_data):
        """Select vendor columns mapped to target columns.

        See VendorTargetMap for more details.
        """
        target_columns = self.vendor_config.vendor_target_map.get_select_list()
        target_data = vendor_data.select(*target_columns)
        return utils.filter_null_xids(target_data)

    def load_and_transform(self):
        """Wrapper around load, transform and map_schema methods."""
        loaded_data = self.load()
        transformed_data = self.transform(loaded_data)
        return self.map_schema(transformed_data)

    def load_transform_write(self):
        """Wrapper around load, transform, map_schema, write methods."""
        transformed_data = self.load_and_transform()
        self.vendor_config.write(transformed_data)
