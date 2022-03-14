import os
from abc import ABC, abstractmethod
from falcon.utils.datetime_utils import DateTimeUtils
from falcon.common import read_brand_exclusions, setup_logging

import pandas as pd
import logging

logger = logging.getLogger(__name__)
setup_logging()


class Brand_Filters_Exclusions(ABC):
    def __init__(self, df, **kwargs):
        self.prediction_traffic_df = df
        self.brand_name = kwargs["brand_name"]
        self.brand_url = kwargs["brand_config"]["brand_alias"]["brand_url"]
        self.mode = kwargs["mode"]
        self.hold_df = kwargs["hold_df"]
        self.multiple_urls_vals_df = kwargs["multiple_urls_vals_df"]
        self.prediction_tags_df = kwargs["prediction_tags_df"]
        self.platform_name = kwargs["platform_name"]

    @abstractmethod
    def brand_filters(self):
        pass

    def brand_exclusions(self):
        try:
            be = read_brand_exclusions()
            exclusions_df = pd.DataFrame(
                be["brand_exclusions"][self.brand_name], columns=["Page"]
            )

            exclusions_df["content_url"] = exclusions_df["Page"].str.replace(
                f"www.{self.brand_url}/", ""
            )
            exclusions_df.drop("Page", inplace=True, axis=1)
            copilot_ids_for_exclusions = self.multiple_urls_vals_df.loc[
                self.multiple_urls_vals_df.copilot_id_urls.isin(
                    exclusions_df.content_url
                ),
                "copilot_id",
            ].values
            logger.info(
                f"Dataframe shape before applying exclusions:  {self.prediction_traffic_df.shape}"
            )
            self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                         ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_exclusions), :
                                         ]
            logger.info(
                f"Dataframe shape after applying exclusions: {self.prediction_traffic_df.shape}"
            )
        except Exception as e:
            logger.info(f"Brand has no exclusion list available !!!")
            pass


class SELF_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        pass


class NEWYORKER_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        # 1. BOROWITZ ISSUE HANDLED HERE (BEFORE IGNORING THE QUEUE FROM PREDICTION DF)
        if (
                self.hold_df.loc[
                    self.hold_df["link"].str.contains("humor/borowitz-report"), "link"
                ].shape[0]
                > 0
                and self.platform_name == "facebook_page"
        ) or (self.platform_name == "twitter"):
            copilot_ids_for_borowitz_content = self.multiple_urls_vals_df.loc[
                self.multiple_urls_vals_df["copilot_id_urls"]
                    .str.lower()
                    .str.contains("humor/borowitz-report"),
                "copilot_id",
            ].values
            self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                         ~self.prediction_traffic_df["cid"].isin(
                                             copilot_ids_for_borowitz_content
                                         ),
                                         :,
                                         ]
            logger.info("Applied BOROWITZ FILTER!!!")

        # 2. DAILY CARTOONS (ONLY ONE SUCH RECOMMENDATION IN THE HOLD QUEUE AT A TIME)
        if (
                self.hold_df.loc[
                    self.hold_df["link"].str.contains(
                        "daily-cartoon|issue-cartoon|cartoons/"
                    ),
                    "link",
                ].shape[0]
                >= 0
        ):
            copilot_ids_for_cartoons = self.multiple_urls_vals_df.loc[
                self.multiple_urls_vals_df["copilot_id_urls"]
                    .str.lower()
                    .str.contains("daily-cartoon|issue-cartoon|cartoons/"),
                "copilot_id",
            ].values
            # logger.info(copilot_ids_for_cartoons)
            self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                         ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_cartoons), :
                                         ]
            logger.info("Applied CARTOONS FILTER!!!")

        # 3. NEWSLETTERS IGNORED FROM URLS
        copilot_ids_for_newsletters = self.multiple_urls_vals_df.loc[
            self.multiple_urls_vals_df["copilot_id_urls"]
                .str.lower()
                .str.contains("newsletters/"),
            "copilot_id",
        ].values
        # logger.info(copilot_ids_for_newsletters)
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_newsletters), :
                                     ]
        logger.info("Applied NEWSLETTER FILTER!!!")

        # 4. POETRY IGNORED FROM URLS
        copilot_ids_for_poetry = self.multiple_urls_vals_df.loc[
            self.multiple_urls_vals_df["copilot_id_urls"]
                .str.lower()
                .str.contains(
                "fiction/poetry|podcast/poetry|puzzles-and-games-dept/crossword|puzzles-and-games-dept/cryptic-crossword"
            ),
            "copilot_id",
        ].values
        # logger.info(copilot_ids_for_poetry)
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_poetry), :
                                     ]
        logger.info("Applied POETRY & PUZZLES FILTER!!!")

        # 5. LETTERS FROM THE IGNORED FROM URLS
        copilot_ids_for_letters = self.multiple_urls_vals_df.loc[
            self.multiple_urls_vals_df["copilot_id_urls"]
                .str.lower()
                .str.contains("letters-from-the"),
            "copilot_id",
        ].values
        # logger.info(copilot_ids_for_letters)
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_letters), :
                                     ]
        logger.info("Applied LETTERS-FROM-THE FILTER!!!")
        logger.info(f"After filters shape {self.prediction_traffic_df.shape}")


class ARCHITECTURALDIGEST_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        # 1. EXCEPTIONS FOR more-news and about-this-week
        copilot_ids_for_url_filters = self.multiple_urls_vals_df.loc[
            self.multiple_urls_vals_df["copilot_id_urls"]
                .str.lower()
                .str.contains("more-news|about-this-week"),
            "copilot_id",
        ].values
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_url_filters), :
                                     ]

        # 2. Subchannel AD100 block
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     self.prediction_traffic_df["subchannel"].str.lower() != "ad100", :
                                     ]

        # 3. AD PRO exclude channel for now
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     self.prediction_traffic_df["channel"].str.lower() != "ad pro", :
                                     ]
        logger.info(f"After filters shape {self.prediction_traffic_df.shape}")

    #         # 3. AD PRO limit to 3 stories on the hold queue at a given time
    #         if (self.hold_df.loc[self.hold_df["link"].str.contains("humor/borowitz-report"), "link"].shape[0] > 0):
    #             copilot_ids_for_borowitz_content = self.multiple_urls_vals_df.loc[self.multiple_urls_vals_df["copilot_id_urls"].str.lower().str.contains("humor/borowitz-report"), "copilot_id"].values
    #             self.prediction_traffic_df = self.prediction_traffic_df.loc[~self.prediction_traffic_df["cid"].isin(copilot_ids_for_borowitz_content), :]


class EPICURIOUS_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        # 1. EXCEPTIONS FOR recipes/member or epicurious/sponsored
        copilot_ids_for_recipes_member = self.multiple_urls_vals_df.loc[
            self.multiple_urls_vals_df["copilot_id_urls"]
                .str.lower()
                .str.contains("recipes/member|sponsored|menus/member"),
            "copilot_id",
        ].values
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_recipes_member), :
                                     ]
        logger.info(f"After filters shape {self.prediction_traffic_df.shape}")


class GQ_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        # 1. EXCEPTIONS FOR big-fit-of-the-day|sponsor-content|best-dressed
        copilot_ids_for_exceptions = self.multiple_urls_vals_df.loc[
            self.multiple_urls_vals_df["copilot_id_urls"]
                .str.lower()
                .str.contains("big-fit-of-the-day|sponsor-content|best-dressed"),
            "copilot_id",
        ].values
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_exceptions), :
                                     ]
        logger.info(f"After filters shape {self.prediction_traffic_df.shape}")

        # 2. GQ Recommend links only published or updated in the last 1 year should be recommended
        gq_recs_minpubdate_epochtime = DateTimeUtils.convert_datetimestr_epochtime(
            DateTimeUtils.get_now_datetime().subtract(days=365).to_date_string()
        )
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~(
                                             (self.prediction_traffic_df["channel"].str.lower() == "gq recommends")
                                             & (
                                                     self.prediction_traffic_df["pub_date_epoch"]
                                                     <= gq_recs_minpubdate_epochtime
                                             )
                                     ),
                                     :,
                                     ]
        logger.info(
            f"After GQ Recs considered for last years filters shape {self.prediction_traffic_df.shape}"
        )


class VOGUE_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        # 1. No parties
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     self.prediction_traffic_df["subchannel"].str.lower() != "parties", :
                                     ]
        logger.info(f"After filters shape {self.prediction_traffic_df.shape}")


class BONAPPETIT_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        # Sponsored channel should not be used for BA
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     self.prediction_traffic_df["channel"].str.lower() != "sponsored", :
                                     ]
        logger.info(f"After filters shape {self.prediction_traffic_df.shape}")


class PITCHFORK_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        # 1. Ignore news/
        copilot_ids_for_news = self.multiple_urls_vals_df.loc[
            self.multiple_urls_vals_df["copilot_id_urls"]
                .str.lower()
                .str.contains("news/"),
            "copilot_id",
        ].values
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_news), :
                                     ]
        logger.info(
            f"After Ignoring News filters shape {self.prediction_traffic_df.shape}"
        )

        # 2. Ignore content with tags of The Ones
        copilot_ids_for_theones = list(
            self.prediction_tags_df.loc[
                self.prediction_tags_df["tags"].str.lower() == "the ones", "cid"
            ]
                .drop_duplicates()
                .values
        )
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~((self.prediction_traffic_df["cid"].isin(copilot_ids_for_theones))),
                                     :,
                                     ]
        logger.info(f"After The Ones filters shape {self.prediction_traffic_df.shape}")

        # 3. Only recommend reviews with rating more than 7.5

    #         if "rating" in prediction_traffic_df.columns:
    #             logger.info("Rating column found and used for filtering in pitchfork")
    #         prediction_traffic_df = prediction_traffic_df.loc[(((prediction_traffic_df["content_type_description"] == "review") & (prediction_traffic_df["rating"].astype("float") >= 7.5)) |
    #                                (prediction_traffic_df["content_type_description"] == "article")), :]


class WIRED_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        # 1. Ignore stories with ideas channel and coronavirus tags
        copilot_ids_for_coronavirus = list(
            self.prediction_tags_df.loc[
                self.prediction_tags_df["tags"].str.lower() == "coronavirus", "cid"
            ]
                .drop_duplicates()
                .values
        )
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~(
                                             (self.prediction_traffic_df["channel"].str.lower() == "ideas")
                                             & (self.prediction_traffic_df["cid"].isin(copilot_ids_for_coronavirus))
                                     ),
                                     :,
                                     ]
        logger.info(
            f"After Coronavirus filters shape {self.prediction_traffic_df.shape}"
        )

        # 2.
        regexp_dt = "today|monday|tuesday|wednesday|thursday|friday|saturday|sunday"
        self.prediction_traffic_df = self.prediction_traffic_df.loc[
                                     ~(
                                             (
                                                 self.prediction_traffic_df["headline"]
                                                     .str.lower()
                                                     .str.contains(regexp_dt)
                                             )
                                             | (
                                                     (~pd.isna(self.prediction_traffic_df["socialcopy"]))
                                                     & (
                                                         self.prediction_traffic_df["socialcopy"]
                                                             .str.lower()
                                                             .str.contains(regexp_dt)
                                                     )
                                             )
                                     ),
                                     :,
                                     ]

        logger.info(
            f"After Days of Week filters shape {self.prediction_traffic_df.shape}"
        )


class VANITYFAIR_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        pass


class TEENVOGUE_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        pass


class CONDENASTTRAVELER_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        # 1. EXCEPTIONS FOR best-bars-in|best-restaurants-in|best-walking-tours-in|best-things-to-do-in|best-hotels-in|best-day-trips-from
        # copilot_ids_for_url_filters = self.multiple_urls_vals_df.loc[
        #     self.multiple_urls_vals_df["copilot_id_urls"]
        #     .str.lower()
        #     .str.contains(
        #         "best-bars-in|best-restaurants-in|best-walking-tours-in|best-things-to-do-in|best-day-trips-from|best-hotels-in|best-museums-in"
        #     ),
        #     "copilot_id",
        # ].values
        # self.prediction_traffic_df = self.prediction_traffic_df.loc[
        #     ~self.prediction_traffic_df["cid"].isin(copilot_ids_for_url_filters), :
        # ]
        pass


class ALLURE_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        pass


class GLAMOUR_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        pass


class VOGUEUK_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        pass


class GQUK_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        pass


class THEM_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        pass


class GQGERMANY_Filters_Exclusions(Brand_Filters_Exclusions):
    def brand_filters(self):
        pass

