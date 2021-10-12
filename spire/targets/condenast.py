from spire.utils.spark_helpers import spark_session
from dataclasses import dataclass
from pyspark.sql.types import DateType, FloatType
import pyspark.sql.functions as F
from pyspark.sql import Window, DataFrame
from spire.targets import constants
from .target_config import TargetConfig
from .vendor_target_map import VendorTargetMap
import datetime


@dataclass
class CondeNastConfig(TargetConfig):
    """
    Parent Config for Covid Passthrough (formerly Covid Proxy) and Affiliates
    Passthrough, and possibly future condenast targets as well.
    """

    date: datetime.date
    n_days: int = 0
    uri: str = constants.GA_STREAM_URI
    input_format: str = "delta"
    vendor_name: str = "condenast"
    source: str = "ga"
    total_article_col: str = "total_articles"

    @spark_session
    def __post_init__(self, spark=None):
        # vendor map initialized here to have access to spark session
        self.vendor_target_map = VendorTargetMap(
            xid=F.col("xid"),
            date=F.col("date").cast(DateType()),
            vendor=F.col("vendor"),
            source=F.col("source"),
            group=F.col("group"),
            aspect=F.col("aspect"),
            value=F.col("value").cast(FloatType()),
        )
        # Need to call super post init method to initialize query
        super().__post_init__()

    def transform(self, data: DataFrame) -> DataFrame:
        """Combines the transformed subclassed passthrough data"""
        covid = CovidPTConfig(self.date)
        affiliate = AffiliatePTConfig(self.date)
        affilate_transformed = affiliate._transform(data)
        covid_transformed = covid._transform(data)
        return covid_transformed.union(affilate_transformed)

    def _transform(self, ga: DataFrame) -> DataFrame:
        """
        Wrapper to transform the subclassed passthrough data according
        to their attributes
        """
        ga = self._preprocess_ga(ga)
        ga = self._filter_and_aggregate(ga)
        ga = self._group_aggregates(ga)
        return self._normalize(ga)

    def _preprocess_ga(self, ga: DataFrame) -> DataFrame:
        """
        Filter infinity_id to the length of GA ids, filter rows with empty content_id
        """
        ga = ga.filter(F.col("dt") == self.date).filter(
            F.length(F.col("infinity_id")) == constants.GA_INFINITY_ID_LENGTH
        )
        return ga.filter(
            (F.col("content_id") != "") & (F.col("content_id").isNotNull())
        )

    def _filter_and_aggregate(self, ga: DataFrame) -> DataFrame:
        """
        - Aggregate by the unique condition for that group (from the sub-class Config)
        - Window function of max date as last_seen over infinity_id
        - Filter to greater than or equal to 30 days before last_seen
        """
        ga = ga.withColumn(self.aggregate_col, self.article_filter_condition)
        ga = ga.withColumn(
            "last_seen", F.max("dt").over(Window.partitionBy("infinity_id"))
        )
        return ga.filter(F.col("dt") >= F.date_sub(F.col("last_seen"), 30))

    def _group_aggregates(self, ga: DataFrame) -> DataFrame:
        """
        A series of group aggregations to get the count of target articles
        and total articles
        """
        return (
            ga.groupby("infinity_id", "content_id", "last_seen")
            .agg(F.max(self.aggregate_col).alias(self.aggregate_col))
            .groupby("infinity_id", "last_seen")
            .agg(
                F.sum(self.aggregate_col).alias(self.target_article_col),
                F.count("*").alias(self.total_article_col),
            )
        )

    def _normalize(self, ga: DataFrame) -> DataFrame:
        """
        Normalizes passthrough targets similarly to map_schema
        """
        return ga.select(
            F.col("infinity_id").alias("xid"),
            F.col("last_seen").alias("date"),
            F.lit(self.vendor_name).alias("vendor"),
            F.lit(self.source).alias("source"),
            F.lit(self.group).alias("group"),
            F.to_json(
                F.create_map(
                    F.lit(self.target_article_col),
                    F.col(self.target_article_col),
                    F.lit(self.total_article_col),
                    F.col(self.total_article_col),
                )
            ).alias("aspect"),
            F.when(F.col(self.target_article_col) >= 1, 1)
            .otherwise(0)
            .cast(FloatType())
            .alias("value"),
        )


class CovidPTConfig(CondeNastConfig):
    group: str = "covid_readers"
    aggregate_col: str = "about_corona"
    target_article_col: str = "corona_articles"

    @spark_session
    def __post_init__(self, spark=None):
        """
        article_filter_condition parameter is a pyspark column type and requires a
        spark session, so it's instantiated using post init to pass it the spark session
        """
        self.article_filter_condition = F.when(
            (F.lower(F.col("hits_page_pagepath")).rlike("coronavirus|covid"))
            | (F.lower(F.col("onsite_keywords")).rlike("coronavirus|covid")),
            1,
        ).otherwise(0)


class AffiliatePTConfig(CondeNastConfig):
    group: str = "affiliate_commerce_clicks"
    aggregate_col: str = "affiliate_network_clicks"
    target_article_col: str = "affiliate_articles"

    @spark_session
    def __post_init__(self, spark=None):
        """
        article_filter_condition parameter is a pyspark column type and requires a
        spark session, so it's instantiated using post init to pass it the spark session
        """
        self.article_filter_condition = F.when(
            F.lower(F.col("hits_eventinfo_eventaction")).isin(
                "affiliate link clicks",
                "amp link type: affiliate link",
                "amp link type: buy button",
            ),
            1,
        ).otherwise(0)
