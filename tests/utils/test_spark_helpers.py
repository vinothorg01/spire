import pytest
import pandas as pd
from spire.utils.spark_helpers import read_with_partition_col


class TestReadWithPartitionCol:
    @pytest.fixture(scope="function")
    def table_uri(self, spark, tmpdir):
        df = spark.createDataFrame(
            pd.DataFrame({"a": [1, 2, 3], "x": [1, 2, 3], "y": [4, 5, 6]})
        )
        table_dir = tmpdir / "table"
        df.write.partitionBy("x", "y").format("parquet").save(str(table_dir))
        return table_dir

    def test_filter_by_partition_in_uri(self, table_uri):
        uri_with_parition = table_uri / "x=1"
        result_df = read_with_partition_col(str(uri_with_parition))

        assert "x" in result_df.columns
        assert result_df.count() == 1

    def test_multiple_partition(self, table_uri):
        uri_with_parition = table_uri / "x=1" / "y=4"
        result_df = read_with_partition_col(str(uri_with_parition))

        assert "x" in result_df.columns
        assert "y" in result_df.columns
        assert result_df.count() == 1
