from ..logger.logger import FalconLogger
from ..utils.datetime_utils import DateTimeUtils
import pandas as pd

class Prediction_Data_Preparation:

    def __init__(self):
        self.logger = FalconLogger()

    def remove_duplicates_hold_brand(self, df):
        pass

    def prepare_custom_data(self, df):
        pass

    def filter_articles_by_published(self, df, brand_config):
        pass

    def add_copilot_ids_to_brand_dfs(self,brand_df, multiple_urls_vals_df):
        pass

    def add_descriptive_boolean_cols(self, prediction_df, exception_df, added_colname=None):
        pass

    def filter_copilots_from_prediciton_data(self, prediction_df, remove_copilot_ids):
        pass


class All_Brand_Data_Preparation(Prediction_Data_Preparation):

    def __init__(self, s_obj):
        self.s_obj = s_obj
        super(All_Brand_Data_Preparation, self).__init__()

    def remove_duplicates_hold_queue(self, hold_brand_df):
        """

        :param hold_brand_df: hold queue articles as dataframe
        :return:
        """
        checker = hold_brand_df.copy()
        valid_content_ids = checker.sort_values("created_ts_epoch", ascending=False).groupby(["cleaned_url"]).first()[
            "content_item_id"].values
        duplicate_content_ids = checker.loc[
            ~checker["content_item_id"].isin(valid_content_ids), "content_item_id"].values

        for content_id in duplicate_content_ids:
            output = self.s_obj.deletequeue([content_id])
            self.logger.info(f"Duplicate content_id {content_id} deleted with status {output['status']}")

        final_df, duplicated_df = checker.loc[checker["content_item_id"].isin(valid_content_ids), :], checker.loc[
                                                                                                      checker[
                                                                                                          "content_item_id"].isin(
                                                                                                          duplicate_content_ids),
                                                                                                      :]
        return final_df, duplicated_df

    def add_copilot_ids_to_brand_dfs(self,brand_df, multiple_urls_vals_df):
        """

        :param brand_df:
        :param multiple_urls_vals_df:
        :return: dataframe
        """
        if brand_df.shape[0] > 0:
            final_brand_df = brand_df.copy()
            social_brand_content_urls = (
                multiple_urls_vals_df.loc[
                multiple_urls_vals_df.copilot_id_urls.isin(
                    final_brand_df.cleaned_url.values
                ),
                :,
                ]
                    .set_index("copilot_id_urls")
                    .groupby("copilot_id_urls")
                    .first()
            )
            social_brand_content_df = pd.merge(
                final_brand_df,
                social_brand_content_urls,
                left_on="cleaned_url",
                right_index=True,
            )

            return social_brand_content_df
        else:
            return brand_df

    def add_descriptive_boolean_cols(self, prediction_df, exception_df, added_colname=None):
        """

        :param prediction_df:
        :param exception_df:
        :param added_colname:
        :return:  dataframe
        """
        prediction_df = prediction_df.copy()
        exception_df = exception_df.copy()

        if exception_df.shape[0] > 0:
            prediction_df.loc[
                prediction_df["cid"].isin(exception_df.copilot_id.values), added_colname
            ] = True
            prediction_df.loc[
                ~prediction_df["cid"].isin(exception_df.copilot_id.values), added_colname
            ] = False
        else:
            prediction_df.loc[:, added_colname] = False
        prediction_df = prediction_df.astype({added_colname: bool})
        return prediction_df

    def filter_copilots_from_prediciton_data(self, prediction_df, remove_copilot_ids):
        """

        :param prediction_df:
        :param remove_copilot_ids:
        :return: dataframe
        """
        self.logger.info("**" * 50)
        self.logger.info(f"BEFORE FILTER 1: {prediction_df.shape}")
        prediction_df = prediction_df.loc[~prediction_df.cid.isin(remove_copilot_ids),
                                :]
        self.logger.info(f"AFTER FILTER 1: {prediction_df.shape}")
        self.logger.info("**" * 50)
        return prediction_df

    def filter_articles_by_published(self, prediction_traffic_df, prediction_brand_config):

        """

        :param prediction_traffic_df:
        :param prediction_brand_config:
        :return: dataframe after filtering by year/data
        """

        if "min_pub_date_year_threshold" in prediction_brand_config["content"].keys():
            num_years = int(prediction_brand_config["content"]["min_pub_date_year_threshold"])
            filter_minpubdate_epochtime = DateTimeUtils.convert_datetimestr_epochtime(
                DateTimeUtils.get_now_datetime().subtract(years=num_years).to_date_string())
        elif "min_pub_date_filter" in prediction_brand_config["content"].keys():
            filter_minpubdate_epochtime = DateTimeUtils.convert_datetimestr_epochtime(
                prediction_brand_config["content"]["min_pub_date_filter"])

        # filter_minpubdate_epochtime
        prediction_traffic_df = prediction_traffic_df.loc[
                                prediction_traffic_df["pub_date_epoch"] >= int(filter_minpubdate_epochtime), :]
        self.logger.info(
            f"Falcon prediction dataframe shape after Pub Date filter: {prediction_traffic_df.shape}"
        )
        prediction_traffic_df = prediction_traffic_df.reset_index(drop=True)
        return prediction_traffic_df


class Wired_Data_Preparation(Prediction_Data_Preparation):
    def __init__(self):
        super(Wired_Data_Preparation, self).__init__()

    def prepare_custom_data(self, hold_brand_df):
        if hold_brand_df.shape[0] > 0:
            self.logger.info(f"Hold df shape is as follows {hold_brand_df.shape[0]}")
            label_counts_df = hold_brand_df.groupby("labels").count()[["link"]]
            label_counts_df = label_counts_df.rename(
                columns={"link": "link_counts"}
            ).reset_index()
            (
                max_wired_gear_value,
                max_wired_science_value,
                max_wired_backchannel_value,
            ) = (3, 3, 2)

            wired_gear_count_df = label_counts_df.loc[
                label_counts_df["labels"].str.contains("falcon")
                & label_counts_df["labels"].str.contains("gear"),
                "link_counts",
            ].values
            wired_gear_value = (
                wired_gear_count_df[0] if wired_gear_count_df.size > 0 else 0
            )
            remaining_wired_gear_value = (
                (max_wired_gear_value - wired_gear_value)
                if (max_wired_gear_value - wired_gear_value) > 0
                else 0
            )

            wired_science_count_df = label_counts_df.loc[
                label_counts_df["labels"].str.contains("falcon")
                & label_counts_df["labels"].str.contains("science"),
                "link_counts",
            ].values
            wired_science_value = (
                wired_science_count_df[0] if wired_science_count_df.size > 0 else 0
            )
            remaining_wired_science_value = (
                (max_wired_science_value - wired_science_value)
                if (max_wired_science_value - wired_science_value) > 0
                else 0
            )

            wired_backchannel_count_df = label_counts_df.loc[
                label_counts_df["labels"].str.contains("falcon")
                & label_counts_df["labels"].str.contains("backchannel"),
                "link_counts",
            ].values
            wired_backchannel_value = (
                wired_backchannel_count_df[0]
                if wired_backchannel_count_df.size > 0
                else 0
            )
            remaining_wired_backchannel_value = (
                (max_wired_backchannel_value - wired_backchannel_value)
                if (max_wired_backchannel_value - wired_backchannel_value) > 0
                else 0
            )
        else:
            (
                remaining_wired_gear_value,
                remaining_wired_science_value,
                remaining_wired_backchannel_value,
            ) = (3, 3, 2)

        return remaining_wired_gear_value, remaining_wired_science_value, remaining_wired_backchannel_value
