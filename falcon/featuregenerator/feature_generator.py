from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from falcon.common import get_min_positive_after_diff
from falcon.features.build_features_kafka import Feature_Selection

class FeatureGenerator:
    """
    Class to generate features for data in Falcon
    Args:
        prediction_brand_config: Brand level prediction configuration
    """
    def __init__(self,prediction_brand_config, logger=None):
        self.prediction_brand_config = prediction_brand_config
        self.get_min_positive_after_diff = F.UserDefinedFunction\
            (get_min_positive_after_diff, IntegerType())
        self.logger = logger


    def sparrow_content_socialflow_data(self, sparrow_data,
                                        final_content_df,
                                        social_brand_content_df):
        """
        Sparrow Data X Content Data on various joins
        """

        join_type = "left_outer"
        join_expression = ((sparrow_data["cid"] == social_brand_content_df["copilot_id"])
                          & (sparrow_data["hours_from_anchor_point"] == social_brand_content_df[
                "social_hours_from_anchor_point"]))
        sparrow_data = sparrow_data.alias("df1").join(social_brand_content_df.alias("df2"), join_expression,
                                                      join_type).select(["df1.*", "df2.Social_Posted"])
        self.logger.info(f"Joining Sparrow data with Social brand content data on {join_type} join...")

        sparrow_data = sparrow_data.na.fill(0)
        sparrow_data = sparrow_data.withColumn("final_social_posted",
                                               F.when(F.col("Social_Posted").isNotNull(), 1).otherwise(0)).drop(
            "Social_Posted")

        # Only keep the rows which have a valid cid available from traffic sparrow data
        join_type = "left_semi"
        join_expression = (sparrow_data["cid"] == final_content_df["copilot_id"])
        joined_traffic_data = sparrow_data.alias('df1').join(final_content_df.alias('df2'), join_expression,
                                                             join_type).select(['df1.*'])
        self.logger.info(f"Joining Sparrow data with Final content data on {join_type} join...")


        # Combine Traffic data to content data to create a final dataframe that can be used to carry out predictions
        join_type = "inner"
        join_expression = (sparrow_data["cid"] == final_content_df["copilot_id"])
        final_df = sparrow_data.alias('df1').join(final_content_df.alias('df2'), join_expression, join_type).select(
            ['df1.*', 'df2.*'])
        self.logger.info(f"Joining Sparrow data with Final content data on {join_type} join...")

        final_df = final_df.na.fill(0)

        return final_df


    def remove_published_before(self, final_df):
        """
        Remove any traffic before the cid has been published(happens a lot with TNY)
        """

        ## hours from anchor point is at what hour this traffic was accessed, and the diff is to calculate its difference from the most recent pubdate and traffic
        checker = final_df.withColumn("pubdate_diff_anchor_point", self.get_min_positive_after_diff(final_df["coll_hfacs"],
                                                                                               final_df[
                                                                                                   "hours_from_anchor_point"]))
        traffic_before_published = checker.where(F.col("pubdate_diff_anchor_point") < 0).count()
        self.logger.info(f"Traffic seen before published {traffic_before_published}")
        self.logger.info(f"Traffic seen before published {traffic_before_published}")

        # if traffic_before_published > 0:
        #     traffic_before_published

        checker = checker.where(
            F.col("pubdate_diff_anchor_point") >= 0)  # removes anything accessed even before being published
        return checker

    def add_features_to_df(self, final_df):
        """
        Feature creation on the content X sparrow X social data
        """
        checker = final_df
        # Recency 1 means new articles - less than 3 weeks older
        # if difference between the traffic accessed and most recent pubdate is more than 504 hours, its old content -> 0
        checker = checker.withColumn("content_recency_when_accessed", F.when(
            ((F.col("pubdate_diff_anchor_point") >= 0) & (F.col("pubdate_diff_anchor_point") <= 504)),
            F.lit("1")).otherwise(F.lit("0")).cast(IntegerType()))
        # Story or Recipe - 0, Gallery and others - 1
        checker = checker.withColumn("content_type_description", F.col("content_type"))
        checker = checker.withColumn("content_type", F.when(
            ((F.col("content_type") == "articles") | (F.col("content_type") == "recipes")), F.lit("0")).otherwise(
            F.lit("1")))

        # Step to Ignore the initial days of new content (Update for new content and old content filter)
        # Way is to ignore data for the initial days (for new content) in hours
        checker = checker.where(
            F.col("pubdate_diff_anchor_point") >= self.prediction_brand_config["content"]["considered_content"][
                "new_content"])
        return checker


    def prepare_logistic_features(self, prediction_df):
        """
        Get the features for the prediciton
        """

        feature_selection = Feature_Selection()
        prediction_df, feature_cols = feature_selection.get_features_logistic_prod(prediction_df, data_for="test")
        return prediction_df, feature_cols