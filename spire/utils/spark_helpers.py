from functools import reduce
from pyspark.sql import SparkSession, DataFrame, functions as F
from delta import configure_spark_with_delta_pip
from spire.config import config
from spire.utils.logger import get_logger


logger = get_logger(__name__)


def init_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName(f"spire-{config.DEPLOYMENT_ENV}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark


def spark_session(func):
    def wrap_spark(*args, **kwargs):
        spark = init_spark()
        kwargs["spark"] = spark
        return func(*args, **kwargs)

    return wrap_spark


def melt(df, id_vars, value_vars, var_name="variable", value_name="value"):
    """Helper function to pivot multiple columns
    of a Spark DataFrame into rows

    Args:
        df : spark.sql.DataFrame: DataFrame to pivot
        id_vars: list: columns to group by
        value_vars: list: columns to use as values
        var_name: string: column name for id_vars
        value_name: string: column name for value_vars

    Returns:
        spark.sql.DataFrame - pivoted
    """

    _vars_and_vals = F.array(
        *(
            F.struct(
                F.lit(c).alias(var_name), F.col("`{}`".format(c)).alias(value_name)
            )
            for c in value_vars
        )
    )
    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))
    cols = id_vars + [
        F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
    ]
    return _tmp.select(*cols)


@spark_session
def read_with_partition_col(uri: str, format: str = "parquet", spark=None) -> DataFrame:
    """Read a table with the partition column included

    For the uri `base_path/id=1`, if you do spark.read.load(uri)
    the partition won't be included in the table. With this method
    it will be included.
    """
    uri_comp = uri.split("/")
    base_uri_comp = []
    filters = []

    # split the url into base_path and filters (partition)
    for comp in uri_comp:
        if "=" in comp:
            col, value = comp.split("=")
            filters.append(F.col(col) == value)
        else:
            if len(filters) > 0:
                raise ValueError("Partition should come after the base table uri.")
            base_uri_comp.append(comp)

    base_uri = "/".join(base_uri_comp)

    # merge the filters into a single filter statement
    filter = reduce(lambda a, b: a & b, filters, F.lit(True))

    return spark.read.format(format).load(base_uri).filter(filter)


@spark_session
def check_if_partitions_exist(table_uri: str, query: str, spark=None) -> bool:
    """Run partition check before deleting"""
    # Collect existing partitions in table
    partition_df = spark.sql(f"""SHOW PARTITIONS delta.`{table_uri}`""")
    # Remove partition for new data if it exists in the targets table partitions
    if partition_df.filter(query).count() > 0:
        return True
    return False


@spark_session
def delete_partition_if_exists(table_uri: str, query: str, spark=None) -> None:
    """Before writing, delete partition for new data if it exists."""

    try:
        partitions_exist = check_if_partitions_exist(table_uri, query)
        if partitions_exist:
            spark.sql(f"""DELETE FROM delta.`{table_uri}` WHERE {query}""")
    except Exception:
        logger.info(f"Table not found at {table_uri}, creating one...")
