import json
import logging
import re
from collections import namedtuple
from multiprocessing import cpu_count, set_start_method, get_context

import requests
from tqdm import tqdm

set_start_method("spawn")
from pprint import pprint

import numpy as np
import pandas as pd
import pendulum
from falcon.common import clean_urls, clean_urls_multiprocessing, setup_logging
from falcon.utils.datetime_utils import DateTimeUtils
from rauth import OAuth1Session
from sqlalchemy import MetaData, Table, delete, insert, select, update
from falcon.database.db_utils import DBUtils

logger = logging.getLogger(__name__)
setup_logging()


class Socialflow:
    def __init__(
            self, socialflow_access, brand_config, db_config, social_platform_name, mode
    ):
        """

        Args:
            socialflow_access:
        """
        self.BASE_URL = "https://www.socialflow.com"
        self.API_BASE_URL = "https://api.socialflow.com"
        self.token = socialflow_access.get("access_token")
        self.access_token_secret = socialflow_access.get("access_token_secret")
        self.consumer_key = socialflow_access.get("consumer_key")
        self.consumer_secret = socialflow_access.get("consumer_secret")

        self.api = OAuth1Session(
            self.consumer_key,
            self.consumer_secret,
            access_token=self.token,
            access_token_secret=self.access_token_secret,
        )

        self.brand_name = brand_config["brand_alias"]["brand_name"]
        self.brand_url = brand_config["brand_alias"]["brand_url"]
        self.brand_shortened_url = brand_config["brand_alias"]["url_shortener"]
        self.platform_name = social_platform_name
        self.mode = mode
        self.db_utils = DBUtils.init_with_db_config(db_config=db_config)

        self.brand_name_to_publish = brand_config["socialflow"][
            "brand_name_to_publish"
        ][
            mode
        ]  # check out that you are passing the mode
        self.brand_name_for_queue = brand_config["socialflow"][
            "brand_name_for_queue"
        ]
        self.brand_name_posted_data = brand_config["socialflow"][
            "posted_data_brand"
        ]

        self.brand_topublish_ids = self._get_brand_ids(
            self.brand_name_to_publish,
            "facebook_page" if self.brand_name_to_publish == "Falcon Test" else self.platform_name,
            db_config,
        )

        self.brand_forqueue_ids = self._get_brand_ids(self.brand_name_for_queue, self.platform_name, db_config
                                                      )
        self.brand_posteddata_ids = self._get_brand_ids(self.brand_name_posted_data, self.platform_name, db_config
                                                        )

        self.shelf_life = brand_config["socialflow"]["shelf_life"]
        self.falcon_labels = brand_config["socialflow"][
            "falcon_labels"
        ]

        self.brand_publish_message_settings = brand_config["socialflow"]["publish_message"]
        self.brand_content_attributes_desc_settings = brand_config["socialflow"]["content_attributes_desc"]

        # reg exp pattern to detect shortened urls
        self.reg_exp_pattern = re.compile(
            r"(https?://({}|t.co|trib.al|trib.al:443)/([a-zA-Z0-9]+))".format(
                self.brand_shortened_url
            )
        )

        # in case where www is present in the link, detect the slug -> like for architectural digest
        # http://www.architecturaldigest.com/{slug}
        self.reg_exp_www_present = re.compile(
            r"(https?://(www.{})/(.*))".format(self.brand_url)
        )

        # in case where www is missing in the link, detect the slug -> like for architectural digest
        # http://architecturaldigest.com/{slug}
        self.reg_exp_www_missing = re.compile(
            r"(https?://({})/(.*))".format(self.brand_url)
        )

        # clean suffixes containing /amp, /all, /1
        self.reg_exp_url_suffix_clean = re.compile(
            r"(/amp$|/all$|/1$)", flags=re.IGNORECASE
        )

    def url_for(self, rel_path):
        """

        Args:
            rel_path:

        Returns:

        """
        return self.BASE_URL + rel_path

    def api_url_for(self, rel_path):
        """

        Args:
            rel_path:

        Returns:

        """
        return self.API_BASE_URL + rel_path

    def _get_brand_ids(self, partner_brand_name, platform_name, db_config):
        """
        This function fails if called from a multithreaded process, when creating the dataset for the first time.
        Run it for any random brand once to populate the datasets and then proceed.
        """
        social_partner_accounts = db_config["social_partner_accounts"]

        # Query 1 :  Get the client_id, client_service_id and service_user_id from db, if it exists
        partner_record = self.db_utils.get_social_partner_accounts(social_partner_accounts, platform_name,
                                                                   partner_brand_name)
        if partner_record is None:
            logger.info("Recreating Database again after calling Socialflow API!!!")
            rowcount = self.db_utils.delete_social_partner_accounts(social_partner_accounts)
            logger.info(
                f"Number of rows deleted from database are {rowcount}"
            )
            logger.info("API Calls for getting IDs again!!!")
            clients_output = self.api.get(
                self.api_url_for("/v1/appuser/user_info")
            ).json()
            for client in clients_output["data"]["user_info"]["client_list"]:
                client_id = client["client_id"]
                brand_outputs = self.api.get(
                    self.api_url_for("/account/list"),
                    params={"client_id": client_id},
                )
                output = brand_outputs.json()
                for client_accounts in output["data"]["client_services"]:
                    if client_accounts["service_type"] != "url_shortening":
                        rowcount = self.db_utils.insert_into_partner_accounts(social_partner_accounts, client_accounts)
                        logger.info("Number of rows inserted into partner accounts : ", rowcount)
            # Query 2 :  Get the client_id, client_service_id and service_user_id from db, if it exists
            partner_record = self.db_utils.get_social_partner_accounts(social_partner_accounts, platform_name,
                                                                       partner_brand_name)
            if partner_record is None:
                raise Exception(
                    "Check the brands for their ids are not available in Socialflow!!!"
                )

        return partner_record

    def sendqueue(self, **kwargs):
        """

        Args:
            brand_service_user_id:
            **kwargs:

        Returns:

        """

        message = kwargs.get("message")
        shorten_links = kwargs.get("shorten_links", 1)
        publish_option = kwargs.get("publish_option", "hold")
        content_attributes = kwargs.get("content_attributes", None)
        expiration_date = kwargs.get("expiration_date")
        created_by = kwargs.get("created_by", "Falcon")

        if content_attributes:
            output = self.api.post(
                self.api_url_for("/message/add"),
                data={
                    "service_user_id": self.brand_topublish_ids.service_user_id,
                    "account_type": self.brand_topublish_ids.social_platform_name,
                    "message": message,
                    "publish_option": publish_option,
                    "shorten_links": shorten_links,
                    "expiration_date": expiration_date,
                    "content_attributes": content_attributes,
                    "created_by": created_by,
                    "client_id": self.brand_topublish_ids.client_id,
                },
            )
        else:
            output = self.api.post(
                self.api_url_for("/message/add"),
                data={
                    "service_user_id": self.brand_topublish_ids.service_user_id,
                    "account_type": self.brand_topublish_ids.social_platform_name,
                    "message": message,
                    "publish_option": publish_option,
                    "shorten_links": shorten_links,
                    "expiration_date": expiration_date,
                    "created_by": created_by,
                    "client_id": self.brand_topublish_ids.client_id,
                },
            )
        return output.json()

    def attach_message_labels(self, content_item_id, label_list):
        """

        Args:
            client_service_id:
            content_item_id:
            label_list:

        Returns:

        """
        output = self.api.post(
            self.api_url_for("/message/label_save"),
            data={
                "client_service_id": self.brand_topublish_ids.client_service_id,
                "content_item_id": content_item_id,
                "labels": ",".join(label_list),
                "client_id": self.brand_topublish_ids.client_id,
            },
        )
        return output.json()

    def deletequeue(
            self,
            delete_itemid_list,
    ):
        """

        Args:
            brand_service_user_id:
            delete_itemid_queue:

        Returns:

        """
        output = self.api.post(
            self.api_url_for("/message/delete"),
            data={
                "service_user_id": self.brand_forqueue_ids.service_user_id,
                "account_type": self.brand_forqueue_ids.social_platform_name,
                "content_item_id": ",".join(str(e) for e in delete_itemid_list),
                "client_id": self.brand_forqueue_ids.client_id,
            },
        )
        return output.json()

    def _get_paginated_results(self, api_url_for_location, params_dict):
        """
        This function returns paginated results for Socialflow API calls
        """
        total_items = []
        current_page = 1
        params_dict["limit"] = 250
        while 1:
            params_dict["page"] = current_page

            output = self.api.get(
                self.api_url_for(api_url_for_location), params=params_dict
            )

            if api_url_for_location == "/contentqueue/list":
                elements = output.json()["data"]["content_queue"]["queue"]
            elif api_url_for_location == "/timeline/get_result":
                elements = output.json()["data"]["items"]
            if len(elements) > 0:
                print(f"Reading page {current_page}")
                total_items.extend(elements)
                current_page += 1
            else:
                break
        return total_items

    def get_socialflow_queues(self):
        """

        Args:
            output:
            brand_config:
        Returns:

        """

        api_url_for_location = "/contentqueue/list"
        params_dict = {
            "service_user_id": self.brand_forqueue_ids.service_user_id,
            "account_type": self.brand_forqueue_ids.social_platform_name,
            "client_id": self.brand_forqueue_ids.client_id,
        }

        output_queue = self._get_paginated_results(
            api_url_for_location=api_url_for_location, params_dict=params_dict
        )

        hold_df = pd.DataFrame(
            columns=[
                "link",
                "feed_message",
                "created_ts_epoch",
                "content_item_id",
                "hold_expiration_ts_epoch",
                "labels",
                "score",
                "type",
            ]
        )
        schedule_df = pd.DataFrame(
            columns=[
                "link",
                "feed_message",
                "created_ts_epoch",
                "content_item_id",
                "scheduled_ts_epoch",
                "labels",
                "score",
                "type",
            ]
        )
        optimize_df = pd.DataFrame(
            columns=[
                "link",
                "feed_message",
                "created_ts_epoch",
                "content_item_id",
                "labels",
                "score",
                "type",
            ]
        )

        hold_list, optimize_list, schedule_list = [], [], []

        for queue_json in output_queue:
            if self.platform_name == "facebook_page":
                try:
                    link_from_post = self.getlink_all(
                        queue_json["content_attributes"]["link"],
                        queue_json["content_attributes"]["message"],
                    )
                    print(f"Link from post {link_from_post}!!!")
                    link = clean_urls(link_from_post)
                    # print(link)
                    if link == "":
                        print("No Url returned, continuing!!!")
                        continue
                except Exception as e:
                    logger.exception(f"Issue with the link, ALERT! ALERT! ALERT! : {e}")
                    continue
            elif self.platform_name == "twitter":
                try:
                    # pprint(queue_json)
                    link_from_post = self.getlink_all("", queue_json["content"])
                    print(f"Link from post {link_from_post}!!!")
                    link = clean_urls(link_from_post)
                    if link == "":
                        print("No Url returned, continuing!!!")
                        continue
                except Exception as e:
                    logger.exception(f"Issue with the link, ALERT! ALERT! ALERT! : {e}")
                    continue
            if self.platform_name == "facebook_page":
                feed_message = queue_json["content_attributes"]["message"]
            elif self.platform_name == "twitter":
                feed_message = queue_json["content"]

            create_ts = DateTimeUtils.convert_socialflow_createtime_epochtime(
                queue_json["create_date"].upper()
            )
            content_item_id = queue_json["content_item_id"]
            score = queue_json["score"]

            try:
                labels = ",".join([str(e) for e in queue_json["meta"]["labels"]])
            except Exception as e:
                logger.exception(
                    f"Issue with the labels, ALERT! ALERT! ALERT! : {e}, link is {link}, will assign label None"
                )
                labels = None

            # print(link, feed_message, create_ts, content_item_id, labels)

            if queue_json["status"] == "Hold":
                #         print("Hold")
                try:
                    expiration_hold_epoch = int(queue_json["meta"]["expire_date_epoch"])
                except Exception as e:
                    # TODO: Ask if these values should be deleted from the queue instead
                    logger.exception(
                        f"Currently ignoring Hold values without hold expiration, ALERT! ALERT! ALERT! : {e}"
                    )
                    logger.exception(
                        f"EXPIRATION HOLD EPOCH TIME AUTOMATICALLY SET TO 10 YEARS IN FUTURE"
                    )
                    expiration_hold_epoch = (
                        DateTimeUtils.convert_datetimestr_datetime(
                            DateTimeUtils.convert_epochtime_todatetime_str(create_ts)
                        )
                            .add(years=10)
                            .int_timestamp
                    )

                hold_list.append(
                    {
                        "link": link,
                        "feed_message": feed_message,
                        "created_ts_epoch": create_ts,
                        "content_item_id": content_item_id,
                        "hold_expiration_ts_epoch": expiration_hold_epoch,
                        "labels": labels,
                        "score": score,
                        "type": "Hold",
                    }
                )

            elif queue_json["status"] == "Schedule":
                # print("Schedule")
                # print(queue_json)
                try:
                    scheduled_date_epoch = int(
                        queue_json["meta"]["scheduled_date_epoch"]
                    )
                except Exception as e:
                    # TODO: Ask if these values should be deleted from the queue instead
                    logger.exception(
                        f"Currently ignoring Schedule values without scheduled time, ALERT! ALERT! ALERT! : {e}"
                    )
                    continue

                schedule_list.append(
                    {
                        "link": link,
                        "feed_message": feed_message,
                        "created_ts_epoch": create_ts,
                        "content_item_id": content_item_id,
                        "scheduled_ts_epoch": scheduled_date_epoch,
                        "labels": labels,
                        "score": score,
                        "type": "Schedule",
                    }
                )

            elif queue_json["status"] == "Optimize":
                #         print("Optimize")

                optimize_list.append(
                    {
                        "link": link,
                        "feed_message": feed_message,
                        "created_ts_epoch": create_ts,
                        "content_item_id": content_item_id,
                        "labels": labels,
                        "score": score,
                        "type": "Optimize",
                    }
                )
            logger.info("**" * 50)
            logger.info("\n\n")

        if len(hold_list) > 0:
            hold_df = hold_df.append(hold_list)
            hold_df = hold_df.replace("", np.nan)
            hold_df["labels"] = hold_df["labels"].fillna("external")

        if len(schedule_list) > 0:
            schedule_df = schedule_df.append(schedule_list)
            schedule_df = schedule_df.replace("", np.nan)
            schedule_df["labels"] = schedule_df["labels"].fillna("external")

        if len(optimize_list) > 0:
            optimize_df = optimize_df.append(optimize_list)
            optimize_df = optimize_df.replace("", np.nan)
            optimize_df["labels"] = optimize_df["labels"].fillna("external")

        hold_brand_df, hold_nonbrand_df = self.sanitize_content_df(
            hold_df, on_column="link"
        )
        schedule_brand_df, schedule_nonbrand_df = self.sanitize_content_df(
            schedule_df, on_column="link"
        )
        optimize_brand_df, optimize_nonbrand_df = self.sanitize_content_df(
            optimize_df, on_column="link"
        )

        return (
            (hold_brand_df, hold_nonbrand_df),
            (schedule_brand_df, schedule_nonbrand_df),
            (optimize_brand_df, optimize_nonbrand_df),
        )

    def _get_posted_data(
            self,
            posted_hours=None,
            start_time=None,
            end_time=None,
            include_items=None,
    ):
        """

        Args:
            posted_hours: For getting the fb posts done in the last x hours. If
                            not specified, you can set a start_time and end_time
            start_time:
            end_time:
            include_items:

        Returns: List of all the entries in the queue or the published list

        """
        if posted_hours and not start_time and not end_time:
            start_time = (
                pendulum.now(tz="UTC").subtract(hours=posted_hours).int_timestamp
            )
            end_time = pendulum.now(tz="UTC").int_timestamp

        if start_time and end_time and not posted_hours:
            start_time = start_time
            end_time = end_time

        if self.platform_name == "facebook_page":
            include_items = "facebook_reaction,facebook_post_stat"
        else:
            include_items = ""

        api_url_for_location = "/timeline/get_result"
        params_dict = {
            "service_user_id": self.brand_posteddata_ids.service_user_id,
            "account_type": self.brand_posteddata_ids.social_platform_name,
            "content_source_ids": "all",
            "start_time": start_time,
            "end_time": end_time,
            "sort_by": "published_date",
            "sort_time": "desc",
            "include": include_items,
            "client_id": self.brand_posteddata_ids.client_id,
        }

        output_list = self._get_paginated_results(
            api_url_for_location=api_url_for_location, params_dict=params_dict
        )

        return output_list

    def get_socialflow_posted_data(
            self,
            posted_data_hours=None,
            start_time=None,
            end_time=None,
    ):
        """[summary]

        Args:
            posted_data_hours ([type], optional): [description]. Defaults to None.
            start_time ([type], optional): [description]. Defaults to None.
            end_time ([type], optional): [description]. Defaults to None.

        Returns:
            [type]: [description]
        """

        if posted_data_hours and not start_time and not end_time:
            print("Posted data hours available, will go from there!!!")
            output_list = self._get_posted_data(
                posted_hours=posted_data_hours,
                start_time=None,
                end_time=None,
                include_items=None,
            )
        if not posted_data_hours and start_time and end_time:
            print("Start time and end time available, will go from there!!!")
            output_list = self._get_posted_data(
                posted_hours=None,
                start_time=start_time,
                end_time=end_time,
                include_items=None,
            )

        try:
            socialdata_list = self._extract_posted_data(output_list)
        except Exception as e:
            print(
                f"Get Socialflow Posted Data has issues with {e}, trying getting the data again in Exception !"
            )
            socialdata_list = self._extract_posted_data(output_list)

        print("Obtained socialposted data from socialflow !!!")

        df = pd.DataFrame(
            socialdata_list,
            columns=["url", "social_created_epoch_time", "socialcopy", "object_id"],
        )
        df["social_created_date"] = pd.to_datetime(
            df["social_created_epoch_time"], unit="s"
        ).dt.date.astype(str)
        df["social_account_type"] = self.platform_name

        social_brand_content_df, social_nonbrand_content_df = self.sanitize_content_df(
            df, on_column="url"
        )
        print("Finishing socialposted function and returning sanitized dataframes")
        return social_brand_content_df, social_nonbrand_content_df

    def check_url_exists(self, urls_list):
        """
        Returns a list of final urls which exist
        """
        final_urls = []
        for url_group in urls_list:
            if requests.get(url_group[0]).status_code == 200:
                final_urls.append(url_group[0])
        return final_urls

    def getlink_all(self, link="", message="") -> (str):
        """
        Get the link from the link or message
        """

        if link != "":
            # if link exists -> either a shortened url which needs unshortening, or a url with www missing
            # Case 1: Shortened URL: re.search will return the first matched element -> which is what is required
            link_valid = re.search(self.reg_exp_pattern, link)
            if link_valid:
                return link_valid.group(1)

            # Case 2: Find the slug in case www is missing and return with www appended. Example, http://architecturaldigest.com/...
            # to http://www.architecturaldigest.com/...
            slug_inter_url_www_missing_link = re.search(self.reg_exp_www_missing, link)
            if slug_inter_url_www_missing_link:
                final_slug_www_missing_link = slug_inter_url_www_missing_link.group(3)
                final_url_www_missing_link = (
                        f"https://www.{self.brand_url}/" + final_slug_www_missing_link
                )
                return final_url_www_missing_link

            # Case 3: Normal expanded url detected
            slug_inter_url_www_present_link = re.search(self.reg_exp_www_present, link)
            if slug_inter_url_www_present_link:
                final_slug_www_present_link = slug_inter_url_www_present_link.group(3)
                final_url_www_present_link = (
                        f"https://www.{self.brand_url}/" + final_slug_www_present_link
                )
                return final_url_www_present_link

        elif (link == "" or "facebook" in link) and message != "":
            # facebook checked for native posts and in native posts, link will include a facebook link but message will have a url
            # when short urls are present in the message
            links_in_message = re.findall(self.reg_exp_pattern, message)
            slug_inter_url_www_present_message = re.search(
                self.reg_exp_www_present, message
            )
            slug_inter_url_www_missing_message = re.search(
                self.reg_exp_www_missing, message
            )

            if (
                    len(links_in_message) == 0
                    and not slug_inter_url_www_missing_message
                    and not slug_inter_url_www_present_message
            ):
                return ""

            # Handle links in message
            if len(links_in_message) == 1:
                # link found in message
                link = links_in_message[0][0]
                return link
            elif len(links_in_message) > 1:
                final_urls = self.check_url_exists(links_in_message)
                if len(final_urls) == 0:
                    return ""
                if len(final_urls) == 1:
                    link = final_urls[0]
                    return link
                else:
                    print(
                        f"Such an exception, message is: {message} -> Here there are multiple urls and all are valid, just returning the first one whose clean_url returns a brand link"
                    )
                    for url in final_urls:
                        int_url = clean_urls(url)

                        # www missing
                        slug_inter_url = re.search(self.reg_exp_www_missing, int_url)
                        if slug_inter_url:
                            final_slug = slug_inter_url.group(3)
                            final_url = f"https://www.{self.brand_url}/" + final_slug
                            print(
                                f"Returning for multiple urls in message with www missing url {url} this {final_url}"
                            )
                            print("* * " * 50)
                            return final_url

                        # www present
                        slug_inter_url = re.search(self.reg_exp_www_present, int_url)
                        if slug_inter_url:
                            final_slug = slug_inter_url.group(3)
                            final_url = f"https://www.{self.brand_url}/" + final_slug
                            print(
                                f"Returning for multiple urls in message with www present url {url} this {final_url}"
                            )
                            print("* * " * 50)
                            return final_url

            # when long url is present in the message with www missing
            if slug_inter_url_www_missing_message:
                final_slug_www_missing_message = (
                    slug_inter_url_www_missing_message.group(3)
                )
                final_url_www_missing_message = (
                        f"https://www.{self.brand_url}/" + final_slug_www_missing_message
                )
                return final_url_www_missing_message

            # when long url is present in the message with www present

            if slug_inter_url_www_present_message:
                final_slug_www_present_message = (
                    slug_inter_url_www_present_message.group(3)
                )
                final_url_www_present_message = (
                        f"https://www.{self.brand_url}/" + final_slug_www_present_message
                )
                return final_url_www_present_message

        else:
            return ""

    def _extract_posted_data(self, posts_data):

        # Function Variables and code to create data
        urls_list = []
        published_date_list = []
        messages_list = []
        objectids_list = []

        for _, data_dict in enumerate(posts_data):
            object_id, link, message, publish_date = "", "", "", ""
            try:
                # get object ids, else skip this item
                object_id = str(data_dict["service_message_id"])
                if object_id == "":
                    continue

                # get published data, else skip this item
                if "meta" in data_dict and "published_date" in data_dict["meta"]:
                    publish_date = str(data_dict["meta"]["published_date"])
                elif "published_date" in data_dict:
                    publish_date = str(data_dict["published_date"])

                if publish_date == "":
                    continue

                if self.platform_name == "facebook_page":
                    if (
                            "content_attributes" in data_dict
                            and "link" in data_dict["content_attributes"]
                    ):
                        link = data_dict["content_attributes"]["link"]

                    if (
                            "content_attributes" in data_dict
                            and "message" in data_dict["content_attributes"]
                    ):
                        message = data_dict["content_attributes"]["message"]
                    elif "content_sent" in data_dict:
                        message = data_dict["content_sent"]

                if self.platform_name == "twitter":
                    # pprint(data_dict)
                    message = data_dict["content_sent"]

                link = self.getlink_all(link, message)
                if link == "":
                    continue

                urls_list.append(link)
                published_date_list.append(publish_date)
                messages_list.append(message)
                objectids_list.append(object_id)

            except Exception as e:
                print(f"Exception in _extract_posted_data: {e}")
                pprint(data_dict)
                print("* *" * 50)
                continue

        url_list_tuples = list(
            zip(urls_list, published_date_list, messages_list, objectids_list)
        )
        print("starting multiprocessing task !!!")
        social_data_list = self._multiprocessing_url_unshortener(url_list_tuples)
        print("finished multiprocessing task !!!")
        return social_data_list

    def _multiprocessing_url_unshortener(self, url_list_tuples):
        """
        the object passed is a list of tuples containing the first column as shortened urls which will be unshortened
        for issues with using get_context("spawn"s)
        https://pythonspeed.com/articles/python-multiprocessing/
        """
        social_data_list = []
        pool = get_context("spawn").Pool(processes=cpu_count())
        for out in tqdm(
                pool.imap_unordered(clean_urls_multiprocessing, url_list_tuples),
                total=len(url_list_tuples),
        ):
            social_data_list.append(out)
        return social_data_list

    def sanitize_content_df(self, df, on_column="url"):
        """Sanitize the content to find out brand content posted on social accounts and also the non brand
        content posted on social accounts for a brand

        Empty URLs become an empty string

        Arguments:
            df {[type]} -- [description]
            brand_config {[type]} -- [description]

        Returns:
            [type] -- [description]
        """
        brand_df = df.copy()
        # Return empty dataframes if passed an empty dataframe
        if brand_df.shape[0] == 0:
            return brand_df, brand_df

        # fill the NaN values
        brand_df[on_column] = brand_df[on_column].fillna("")
        non_brand_df = brand_df.loc[
                       ~brand_df[on_column].str.contains(f"{self.brand_url}"),
                       :,
                       ]
        brand_df = brand_df.loc[
                   brand_df[on_column].str.contains(f"{self.brand_url}"),
                   :,
                   ]
        brand_df["cleaned_url"] = brand_df[on_column].str.extract(
            r"({}/(?P<suffix>.*))".format(self.brand_url)
        )["suffix"]
        brand_df["cleaned_url"] = brand_df["cleaned_url"].str.strip()
        brand_df["cleaned_url"] = brand_df["cleaned_url"].str.rstrip("/")
        brand_df["cleaned_url"] = brand_df["cleaned_url"].str.lstrip("/")
        brand_df["cleaned_url"] = brand_df["cleaned_url"].str.replace(
            self.reg_exp_url_suffix_clean, ""
        )
        brand_df[on_column] = f"https://www.{self.brand_url}/" + brand_df["cleaned_url"]
        brand_df.reset_index(inplace=True, drop=True)
        return brand_df, non_brand_df

    def post_on_socialflow(self, outcomes_socialflow, falcon_config):

        if self.mode == "dev":
            self.shelf_life = 1

        tz = pendulum.timezone("America/New_York")
        total_posts = 0

        for falcon_outcome in outcomes_socialflow.itertuples(
                index=True, name="Falcon_Outcome"
        ):
            # prediction_cid = getattr(falcon_outcome, "cid")
            try:
                final_url = getattr(falcon_outcome, "long_url")
                check_url = self.getlink_all(link=clean_urls(final_url), message="")
                if final_url == check_url:
                    print(
                        "Function results in final url and redirect url being the same :::: ",
                        final_url,
                        check_url,
                    )
                else:
                    if requests.get(check_url, stream=True).status_code == 200:
                        print(
                            "Redirect url is different from given url and redirect url exists, using it::::"
                        )
                        final_url = check_url
                    else:
                        print(
                            f"Error as Redirect URL does not exist for URL {final_url} whose redirect URL is {check_url}"
                        )
                        continue

                final_url += f"?utm_campaign={falcon_config['utm_campaign']}"
            except Exception as e:
                print(
                    f"Posting on Socialflow results in an error as link cannot be formed {e}"
                )

            feed_message = ""

            for vals in self.brand_publish_message_settings:
                if getattr(falcon_outcome, "socialcopy"):
                    if vals == "pub_date_epoch":
                        pub_date_epoch_dt = pendulum.from_timestamp(
                            getattr(falcon_outcome, vals), tz=tz
                        )
                        diff_for_humans = (
                            pendulum.now(tz="UTC")
                                .subtract(
                                seconds=(
                                        pendulum.now(tz="UTC").int_timestamp
                                        - getattr(falcon_outcome, vals)
                                )
                            )
                                .diff_for_humans()
                        )
                        feed_message += f"Last Copilot Revision Date: {pub_date_epoch_dt.to_date_string()}({diff_for_humans})"
                    elif vals == "social_created_epoch_time":
                        soccopy_pub_date_epoch_dt = pendulum.from_timestamp(
                            getattr(falcon_outcome, vals), tz=tz
                        )
                        diff_for_humans = (
                            pendulum.now(tz="UTC")
                                .subtract(
                                seconds=(
                                        pendulum.now(tz="UTC").int_timestamp
                                        - getattr(falcon_outcome, vals)
                                )
                            )
                                .diff_for_humans()
                        )
                        feed_message += f"Last {'Facebook' if self.platform_name == 'facebook_page' else 'Twitter'} Post Date: {soccopy_pub_date_epoch_dt.to_date_string()}({diff_for_humans})\n"
                    elif vals == "content_url":
                        feed_message += f"{vals.title()}: {final_url}\n"
                    else:
                        if vals == "socialtitle" and not getattr(falcon_outcome, vals):
                            logger.info(
                                "FOR VALUES HERE WHERE SOCIALCOPY EXISTS AND SOCIALTITLE DOES NOT EXIST"
                            )
                            feed_message += (
                                f"Headline: {getattr(falcon_outcome, 'headline')} \n"
                            )
                        else:
                            # this is where socialcopy gets written, replace old socialcopy url in twitter with new url
                            # also deal with multiple urls, remove all urls other than the first one, which gets replaced by new
                            socialcopy = getattr(falcon_outcome, vals)
                            if self.platform_name == "twitter":
                                links_in_socialcopy = re.findall(
                                    self.reg_exp_pattern, socialcopy
                                )
                                logger.info(
                                    f"NUMBER OF URLS IN THE OLD TWEET: {len(links_in_socialcopy)}"
                                )
                                if len(links_in_socialcopy) > 0:
                                    for i, link_tuple in enumerate(links_in_socialcopy):
                                        print(link_tuple)
                                        if i == 0:
                                            socialcopy = socialcopy.replace(
                                                link_tuple[0], final_url
                                            )
                                        else:
                                            socialcopy = socialcopy.replace(
                                                link_tuple[0], ""
                                            )
                                feed_message += f"{vals.title()}: {socialcopy}\n"
                            else:
                                feed_message += f"{vals.title()}: {socialcopy}\n"
                else:
                    if vals == "socialcopy" or vals == "social_created_epoch_time":
                        continue
                    if vals == "pub_date_epoch":
                        pub_date_epoch_dt = pendulum.from_timestamp(
                            getattr(falcon_outcome, vals), tz=tz
                        )
                        diff_for_humans = (
                            pendulum.now(tz="UTC")
                                .subtract(
                                seconds=(
                                        pendulum.now(tz="UTC").int_timestamp
                                        - getattr(falcon_outcome, vals)
                                )
                            )
                                .diff_for_humans()
                        )
                        feed_message += f"\nLast Copilot Revision Date: {pub_date_epoch_dt.to_date_string()}({diff_for_humans})"
                    elif vals == "content_url":
                        feed_message += f"{final_url}"
                    else:
                        if vals == "socialtitle" and not getattr(falcon_outcome, vals):
                            logger.info(
                                "FOR VALUES HERE WHERE SOCIALCOPY DOES NOT EXIST AND SOCIALTITLE ALSO DOES NOT EXIST"
                            )
                            feed_message += f"{getattr(falcon_outcome, 'headline')} "
                        else:
                            feed_message += f"{getattr(falcon_outcome, vals)} "

            logger.info(f"Feed Message is {feed_message}")

            final_message = dict()
            for k, v in self.brand_content_attributes_desc_settings.items():
                if k == "link":
                    final_message[k] = final_url
                elif v == "socialtitle" and not getattr(falcon_outcome, v):
                    final_message[k] = getattr(falcon_outcome, "headline")
                elif v == "socialdescription" and not getattr(falcon_outcome, v):
                    final_message[k] = getattr(falcon_outcome, "dek")
                else:
                    final_message[k] = getattr(falcon_outcome, v)

            content_attributes_message = json.dumps(final_message)

            logger.info(f"Headline is {getattr(falcon_outcome, 'headline')}")
            logger.info(f"Dek is {getattr(falcon_outcome, 'dek')}")
            logger.info(f"Socialtitle is {getattr(falcon_outcome, 'socialtitle')}")
            logger.info(
                f"Socialdescription is {getattr(falcon_outcome, 'socialdescription')}"
            )
            logger.info(f"URL is {final_url}")

            assert "utm_campaign=falcon" in final_url

            publish_kwargs = {
                "shorten_links": 1,
                "message": feed_message,  # .replace("\"", "").replace("\'", ""),
                "publish_option": "hold",
                "expiration_date": pendulum.now(tz="America/New_York")
                    .add(hours=self.shelf_life)
                    .to_iso8601_string(),
                "created_by": "Falcon",
                "content_attributes": content_attributes_message,
            }

            # Assigning special labels to track posts
            if self.brand_name == "wired" and getattr(falcon_outcome, "channel") in [
                "Gear",
                "Science",
                "Backchannel",
            ]:
                post_label_list = [getattr(falcon_outcome, "channel").lower(), "falcon"]
            else:
                post_label_list = ["falcon"]

            out = self.sendqueue(**publish_kwargs)

            if out["status"] == 200:
                # change this variable name
                logger.info(
                    f"Published to socialflow on hold : Feed Message is -> {feed_message}"
                )
                output = self.attach_message_labels(
                    content_item_id=out["data"]["content_item"]["content_item_id"],
                    label_list=post_label_list,
                )
                if output["status"] == 200:
                    for label in post_label_list:
                        logger.info(f"Attached {label} Label !!\n")
                    logger.info("** " * 50)
                    logger.info("\n\n")
                    total_posts += 1
                else:
                    logger.info("Issue attaching Falcon Label !!\n")
            else:
                logger.info(f"Could not publish {out}")
        else:
            logger.info("** " * 50)
            logger.info("\n\n")

        return total_posts
