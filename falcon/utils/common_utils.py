from pyspark.sql import functions as F
from pyspark.sql.types import StringType,TimestampType,IntegerType
from pyspark.sql.window import Window

class CommonUtils:

    @staticmethod
    def get_groupby_max(df, timestamp_col, partition_col):
        df_with_unixtime = df.withColumn("unixtime_pubdate", F.col(timestamp_col))
        w = Window().partitionBy(partition_col).orderBy(F.desc("unixtime_pubdate"))
        df_with_rank = (df_with_unixtime.withColumn("rank", F.dense_rank().over(w)))
        result = df_with_rank.where(F.col("rank") == 1)
        result = result.drop(F.col("rank"))
        result = result.drop(F.col("unixtime_pubdate"))
        return result

    @staticmethod
    def get_definedhours_from_anchorpoint_pyspark(df, output_colname, to_ts_col, anchor_ts="2016-01-01"):
        df = df.withColumn(to_ts_col, F.col(to_ts_col).cast(StringType()))
        df = df.withColumn(output_colname, (((F.unix_timestamp(
            F.col(to_ts_col).cast(TimestampType())) - F.unix_timestamp(
            F.lit(anchor_ts).cast(TimestampType()))) / 3600).cast(IntegerType())))
        df = df.withColumn(to_ts_col, F.col(to_ts_col).cast(TimestampType()))
        return df
