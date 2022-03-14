import requests
from ..logger.logger import FalconLogger
import sys

class PublishToSocialflow():

    def __init__(self, s_obj, falcon_config, outcomes):
        self.logger = FalconLogger()
        self.s_obj = s_obj
        self.falcon_config = falcon_config
        self.outcomes_socialflow_identifiers = outcomes
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}

    def publish_outcomes_to_socialflow(self):
        """

        :return: None
        """
        outcomes_socialflow_identifiers = self.outcomes_socialflow_identifiers
        outcomes_socialflow_identifiers["long_url"] = f"https://www.{self.s_obj.brand_url}/" + \
                                                      outcomes_socialflow_identifiers["copilot_id_urls"]
        outcomes_socialflow_identifiers["url_requests"] = outcomes_socialflow_identifiers["long_url"].apply(
            lambda x: requests.get(x, headers=self.headers, stream=True).status_code)

        outcomes_socialflow_identifiers = outcomes_socialflow_identifiers.loc[((outcomes_socialflow_identifiers[
                                                                                    "url_requests"] == 200) | (
                                                                                       outcomes_socialflow_identifiers[
                                                                                           "url_requests"] == 402)), :]

        outcomes_socialflow_identifiers = outcomes_socialflow_identifiers.drop_duplicates(keep="first", subset=["cid"])
        outcomes_socialflow_identifiers = outcomes_socialflow_identifiers.reset_index(drop=True)

        if outcomes_socialflow_identifiers.shape[0] == 0:
            self.logger.warning("Nothing is available to make a post, break it!!!")
            sys.exit(1)
        else:
            print(f"Count of posts for this hour is {outcomes_socialflow_identifiers.shape[0]}")

            total_posts_on_socialflow = self.s_obj.post_on_socialflow(outcomes_socialflow_identifiers,
                                                                      self.falcon_config)

        if total_posts_on_socialflow == 0:
            self.logger.warning("No socialflow posts made for this hour, check this run!!!")
            sys.exit(1)
        else:
            self.logger.info(f"Count of socialflow posts for this hour is {total_posts_on_socialflow}")
