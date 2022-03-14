import os
from pyspark.sql import functions as F
from falcon.common import read_config
from falcon.logger.logger import FalconLogger


class SparrowDataReader:

    def __init__(self,
                 is_train,
                 brand_name,
                 hfac,
                 social_account_type = 'facebook_page',
                 mode='dev',
                 brand_config=None,
                 spark=None
                 ):

        self.is_train = is_train
        self.hfac = hfac
        self.brand_name = brand_name
        self.social_account_type = social_account_type
        self.mode = mode
        self.falcon_config = read_config("falcon")
        self.prediction_brand_config = brand_config
        self.spark=spark
        self.logger_obj = FalconLogger()


    def read_sparrow_data(self):
        """
        Reads the sparrow records from the Test data path.
        :return: DataFrame
        """
        brand_sparrow = self.prediction_brand_config["brand_alias"]["sparrow"]
        self.logger_obj.info(f'The brand name for Sparrow data is: {brand_sparrow}')

        if self.is_train:
            ## Training Data
            data_location_config = self.falcon_config["train_data"]["s3_bucket"][self.mode]
            train_data_path = os.path.join(data_location_config, "latest")
            self.logger_obj.info(f'The is_train param is True... Collecting the training data from {train_data_path}')

            sparrow_data = self.spark.read.format("delta").load(train_data_path).where(F.col("brand") == brand_sparrow)
        else:
            ## Prediction Data
            try:
                data_location_config = self.falcon_config["test_data"]["s3_bucket"][self.mode]
                self.logger_obj.info(f'The is_train param is False... Collecting the test data from {data_location_config} for hfac={self.hfac}')

                sparrow_data = self.spark.read.format("delta").load(data_location_config).where(
                    F.col("brand") == brand_sparrow).where(F.col("hfac") == self.hfac).drop("hfac")
                if sparrow_data.count() == 0:
                    self.logger_obj.info(f'No sparrow data found for hfac={self.hfac}... Collecting the test data for hfac={self.hfac-1}')
                    self.hfac -= 1

                    sparrow_data = self.spark.read.format("delta").load(data_location_config).where(
                        F.col("brand") == brand_sparrow).where(F.col("hfac") == self.hfac).drop("hfac")
                # Last x hours of data used for prediction(Currently x=10, can be changed in settings.yaml)
                sparrow_data = sparrow_data.where(
                    F.col("hours_from_anchor_point") >= self.hfac - self.prediction_brand_config["traffic"]["traffic_hours"])
            except Exception as e:
                print(e)
                self.logger_obj.error(f"Failure to load prediction data \n{e}")

        min_data_hfac = sparrow_data.select(F.min("hours_from_anchor_point")).collect()[0][0]
        max_data_hfac = sparrow_data.select(F.max("hours_from_anchor_point")).collect()[0][0]
        return sparrow_data.drop(F.col("brand")), min_data_hfac, max_data_hfac
