import os
from pyspark.sql import functions as F
from pyspark.sql.types import *
from falcon.common import read_config
from falcon.utils.datetime_utils import DateTimeUtils
from falcon.utils.common_utils import CommonUtils
from falcon.dataset.socialcopy_data_kafka import update_socialcopy_db_data
from falcon.logger.logger import FalconLogger

class SocialDataReader:
    """
    Class for updating socialcopy data, uploading updated socialcopy on S3, reading socialcopy from s3
    Args:
        db_config: Database Config
         is_train: Data for training or prediction, True is training data, False is prediction data
         brand_name: Name of brand for data
         social_platform_name: Name of Social Platform {Facebook, Twitter}
         prediction_brand_config: Brand lkevel prediction config
         s_obj: Socialflow Object
         social_partner_name: Name of Social Partner {Socialflow}
         db_utils: Database Utils object
         mode: The running mode {dev, staging, prod}
         spark: Spark object

    """

    def __init__(self, db_config,
                 is_train,
                 brand_name,
                 social_platform_name,
                 prediction_brand_config,
                 s_obj,
                 social_partner_name,
                 db_utils,
                 mode='dev',
                 spark=None
                 ):

        self.is_train = is_train
        self.brand_name = brand_name
        self.brand_id = db_utils.get_brand_id(brand_name)
        self.social_platform_name = social_platform_name
        self.prediction_brand_config = prediction_brand_config
        self.mode = mode
        self.s_obj = s_obj
        self.social_platform_id = db_utils.get_social_platform_id(social_platform_name)
        self.social_partner_name=social_partner_name

        self.socialcopy_config = read_config(settings_name="socialcopy_data")
        self.traffic_config = read_config(settings_name="traffic")
        self.posted_data_hours_epoch_starttime = DateTimeUtils.get_now_datetime().subtract(
            hours=prediction_brand_config["socialflow"]["posted_data_hours"]).int_timestamp
        self.db_config = db_config
        self.spark =spark
        self.logger = FalconLogger()

        self.socialcopy_bucket_path = os.path.join(self.socialcopy_config["data"]["s3_bucket"][self.mode], self.social_platform_name)

    def update_and_read_socialcopy_data(self):
        # Update the new social posts added to the data location
        """

        :return: updated socialcopy as dataframe
        """
        self._update_socialcopies(self.socialcopy_bucket_path)
        self.logger.info("Reading Social Copy Data for the Social Account...")
        socialcopy_df = self.spark.read.format("delta").load(self.socialcopy_bucket_path).where(F.col("brand") == self.brand_name)
        self.logger.info("Finished Reading Social Copy Data for the Social Account!!!")

        return socialcopy_df

    def _update_socialcopies(self, socialcopy_bucket_path):
        """
        If posted data hours is passed as an integer value, gets the posted data in the last x hours and returns the dataframe.
        """

        self.logger.info("Updating Social Copy Database for the Social Account...")

        social_brand_content_df, non_social_brand_content_df, first_time_update = update_socialcopy_db_data(
            db_config=self.db_config,
            social_platform_id=self.social_platform_id,
            brand_id=self.brand_id,
            social_partner_object=self.s_obj,
            socialcopy_config=self.socialcopy_config,
            social_platform_name=self.social_platform_name,
            social_partner=self.social_partner_name,
        )
        social_brand_content_df_shape = social_brand_content_df.shape

        self.logger.info(
            f"Count of latest SocialCopy Added articles for Social brand content: {social_brand_content_df_shape}"
        )

        if social_brand_content_df_shape[0] > 0:
            self.logger.info("Status of Social Copy Datastore update: {}".format(self._update_socialcopy_datastore(social_brand_content_df, socialcopy_bucket_path, first_time_update)))

        else:
            self.logger.info("No socialcopy for Social brand content updates from the last timestamp recorded!!!")

        non_social_brand_content_df_shape = non_social_brand_content_df.shape
        if non_social_brand_content_df_shape[0] > 0:
            self.logger.info(
                f"Count of latest SocialCopy Added articles for Non-Social brand content: {non_social_brand_content_df_shape}"
            )
            # self.logger.info(non_social_brand_content_df.show())

    def _update_socialcopy_datastore(self, social_brand_content_df, socialcopy_bucket_path, first_time_update):

        socialcopy_schema = StructType([StructField("url", StringType(), True), \
                                       StructField("social_created_epoch_time", StringType(), True), \
                                       StructField("socialcopy", StringType(), True), \
                                       StructField("object_id", StringType(), True), \
                                       StructField("social_created_date", StringType(), True), \
                                       StructField("social_account_type", StringType(), True), \
                                       StructField("cleaned_url", StringType(), True)
                                       ])
        self.logger.info(f"Creating SocialCopy Dataframe from Social brand content data with following schema: {socialcopy_schema}")
        socialcopy_spark_df = self.spark.createDataFrame(social_brand_content_df, schema=socialcopy_schema)
        socialcopy_spark_df = socialcopy_spark_df.withColumn("social_created_date",
                                                             F.col("social_created_date").cast(DateType())).withColumn(
            "brand", F.lit(self.brand_name))
        socialcopy_spark_df = socialcopy_spark_df.withColumn("social_created_epoch_time",
                                                             F.col("social_created_epoch_time").cast(IntegerType()))

        #   display(socialcopy_spark_df)

        if first_time_update:
            # Overwrite the delta partitions
            self.logger.info("This is a first time update of socialcopy datastore!!!")
            socialcopy_spark_df.write.partitionBy("brand").format("delta").mode("overwrite").option("replaceWhere",
                                                                                                    f"brand='{self.brand_name}'").save(
                socialcopy_bucket_path)

        else:
            # append to the delta location
            socialcopy_spark_df.write.partitionBy("brand").format("delta").mode("append").save(socialcopy_bucket_path)

        self.logger.info(f"Updated SocialCopy Datastore at {socialcopy_bucket_path}")
        return "Success"


    def _retrieve_socialcopies(self, socialcopy_bucket_path, epoch_starttime):
        self.logger.info(f"Retreiving the social copy data from {socialcopy_bucket_path}")
        #In line number 51 (_update_and_read_socialcopy_data function) also we are reading social copy data from socialcopy_bucket_path, so what's the difference?
        final_df = self.spark.read.format("delta").load(socialcopy_bucket_path).where(F.col("brand") == self.brand_name)
        final_df = final_df.where(F.col("social_created_epoch_time") >= epoch_starttime)
        final_df = final_df.withColumn("social_created_time_utc",
                                       final_df.social_created_epoch_time.cast(TimestampType()))
        return final_df

    def get_socialposted_brand_data(self, identifier_urls):

        """

        :param identifier_urls: dataframe with content urls
        :return: social_brand_content_df
        """
        epoch_starttime = DateTimeUtils.convert_datetimestr_epochtime(
                "2021-01-01") if self.is_train else self.posted_data_hours_epoch_starttime

        self.logger.info(f"The is_train param is {self.is_train}... Collecting the Social Posted Brand data with epoch starttime:"
                              f"{epoch_starttime} from {self.socialcopy_bucket_path}")

        social_brand_content_df = self._retrieve_socialcopies(socialcopy_bucket_path=self.socialcopy_bucket_path,
                                                             epoch_starttime=epoch_starttime)
        social_brand_content_df = social_brand_content_df.select(
            [*[c for c in social_brand_content_df.columns], F.lit("Social_Posted")])

        ## Content recently posted on social accounts but cids are not available(error check), add to dashboard for metrics
        # read identifier urls

        # _, identifier_urls = self._read_content_data()
        join_type = "left_anti"
        join_expression = social_brand_content_df["cleaned_url"] == identifier_urls["copilot_id_urls"]
        error_df = social_brand_content_df.alias('df1').join(identifier_urls.alias('df2'), join_expression,
                                                             join_type).select(['df1.*'])

        self.logger.info(f"Joining Social brand data with Identifier URLs on {join_type} join...")


        ### MAIN CODE, content recently posted with cids available
        join_type = "inner"
        join_expression = social_brand_content_df["cleaned_url"] == identifier_urls["copilot_id_urls"]
        social_brand_content_df = social_brand_content_df.alias('df1').join(identifier_urls.alias('df2'),
                                                                            join_expression, join_type).select(
            ['df1.*', 'df2.copilot_id'])
        self.logger.info(f"Joining Social brand data with Identifier URLs on {join_type} join...")

        ### TODO: can put the count of this in redash to track every hour

        ## adding column to include social hours from anchor point from anchor date
        anchor_date = self.traffic_config["anchor_date"]
        social_brand_content_df = CommonUtils.get_definedhours_from_anchorpoint_pyspark(social_brand_content_df,
                                                                            output_colname="social_hours_from_anchor_point",
                                                                            to_ts_col="social_created_time_utc",
                                                                            anchor_ts=anchor_date)

        return social_brand_content_df, error_df
