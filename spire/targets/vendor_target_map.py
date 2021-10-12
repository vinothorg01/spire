# TODO: (Astha) Add column type validators
from pydantic import BaseModel
from pyspark.sql.column import Column
from typing import List


class VendorTargetMap(BaseModel):
    """Target schema mapped to vendor specific columns.

    VendorTargetMap objects are created inside vendor configs
    and map vendor cols --> target cols.
    """

    class Config:
        """Alters the config to allow pyspark columns as a data type."""

        arbitrary_types_allowed = True

    xid: Column
    date: Column
    vendor: Column
    source: Column
    group: Column
    aspect: Column
    value: Column

    def get_select_list(self) -> List[Column]:
        """Return list of renamed vendor columns to select from transformed data."""
        return [vendor.alias(target) for target, vendor in self.dict().items()]
