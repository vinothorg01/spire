from pyspark.sql import functions as F
from falcon.common import read_config
from falcon.utils.common_utils import CommonUtils
from falcon.logger.logger import FalconLogger


class ContentDataReader:
    """
    Class for reading Content Data from S3
    Args:
        brand_config: Brand level Config
        mode: The running mode {dev, staging, prod}
        spark: Spark object
    """

    def __init__(self,
                 brand_config,
                 mode='dev',
                 spark= None
                 ):

        self.mode = mode
        self.prediction_brand_config = brand_config
        self.content_config = read_config(settings_name="content")
        self.spark = spark
        self.logger_obj = FalconLogger()

    def read_content_data(self):
        """

        :return: DataFrame
        """
        aleph_k2d_brandname = self.prediction_brand_config["brand_alias"]["aleph_k2d"]
        data_location_config = self.content_config["data"]["s3_bucket"][self.mode]
        self.logger_obj.info(f"Reading Content data for {aleph_k2d_brandname} from {data_location_config}...")

        content_joined_df = self.spark.read.format("delta").load(data_location_config).where(
            F.col("brand") == aleph_k2d_brandname)
        identifier_urls = content_joined_df.select(F.col("copilot_id"),
                                                   F.explode("identifier_urls").alias("copilot_id_urls")).distinct()
        return content_joined_df, identifier_urls

    def get_content_data_with_latest_socialcopy(self, socialcopy_df, content_joined_df, identifier_urls):

        # Connect urls to copilot ids
        """

        :param socialcopy_df: updated socialcopy dataframe
        :param content_joined_df: content dataframe
        :param identifier_urls: identifiers url dataframe
        :return: dataframe with content joined with socialcopy with urls mapped.
        """
        join_type = "left"
        join_expression = socialcopy_df["cleaned_url"] == identifier_urls["copilot_id_urls"]
        socialcopy_cid_df = socialcopy_df.alias('df1').join(identifier_urls.alias('df2'), join_expression,
                                                            join_type).select(['df1.*', 'df2.copilot_id'])

        self.logger_obj.info(f"Joining Social Copy data with Identifier URLs on {join_type} join...")

        # Error counts of the number of missing copilot_ids in every run, track these in a dashboard for model tracking - how many urls are missing cids
        #     socialcopy_cid_df.where(F.col("copilot_id").isNull()).count()  # instead of printing this, can just be tracked with the hfac in a database for model performance metrics

        # null handling where copilot ids don't exist, can be tracked in a dashboard
        socialcopy_cid_df = socialcopy_cid_df.where(F.col("copilot_id").isNotNull())

        # Gets the most recently used socialcopy for every copilot id
        socialcopy_cid_df = CommonUtils.get_groupby_max(socialcopy_cid_df, timestamp_col="social_created_epoch_time",
                                                        partition_col="copilot_id")

        # check the counts, can be tracked for the runs to see how it varies with every hour to see for errors in a dashboard
        #     socialcopy_cid_df.count()

        # content x socialflow data
        join_type = "left"
        join_expression = (content_joined_df["copilot_id"] == socialcopy_cid_df["copilot_id"])
        final_content_df = content_joined_df.alias('df1').join(socialcopy_cid_df.alias('df2'), join_expression,
                                                               join_type).select(
            ['df1.*', 'df2.socialcopy', 'df2.social_created_epoch_time'])

        self.logger_obj.info(f"Getting Content data with latest Social Copy data using {join_type} join...")

        return final_content_df

