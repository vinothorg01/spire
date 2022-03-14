import pandas as pd
from ..logger.logger import FalconLogger


class BRAND_RECOMMENDATIONS():

    def __init__(self, outcomes, min_articles_recipes, min_gallery_reviews):
        self.logger = FalconLogger()
        self.min_articles_recipes = min_articles_recipes
        self.min_gallery_reviews = min_gallery_reviews
        self.outcomes = outcomes

    def filter_by_content_recency(self, content_type, recency_accessed):
        '''

        :param content_type: 0, 1 (article/gallery)
        :param recency_accessed: 0,1(New/Old)
        :return: outcomes
        '''
        outcomes = self.outcomes
        return outcomes.loc[
               (
                       (outcomes["content_type"].astype(int) == content_type)
                       & (outcomes["content_recency_when_accessed"].astype(int) == recency_accessed)
               ),
               :,
               ].reset_index(drop=True)

    def _filter_new_old_articles(self):
        """

        :return: new_articles, old_articles, new_galleries, old_galleries
        """
        outcomes_for_articles_recipes_new = self.filter_by_content_recency(content_type=0, recency_accessed=1)
        outcomes_for_articles_recipes_old = self.filter_by_content_recency(content_type=0, recency_accessed=0)
        outcomes_for_galleries_reviews_new = self.filter_by_content_recency(content_type=1, recency_accessed=1)
        outcomes_for_galleries_reviews_old = self.filter_by_content_recency(content_type=1, recency_accessed=0)

        return outcomes_for_articles_recipes_new, outcomes_for_articles_recipes_old, \
               outcomes_for_galleries_reviews_new, outcomes_for_galleries_reviews_old

    def _get_recs_based_even_hour(self, outcomes_for_articles_recipes_old,
                                outcomes_for_galleries_reviews_new,
                                outcomes_for_galleries_reviews_old):
        """

        :param outcomes_for_articles_recipes_old:
        :param outcomes_for_galleries_reviews_new:
        :param outcomes_for_galleries_reviews_old:
        :return:  old article, new gallery
        """
        self.logger.info("Even Hour Calculations")
        outcomes_for_articles_recipes = outcomes_for_articles_recipes_old[
                                        :self.min_articles_recipes
                                        ]

        remaining_gallery_reviews = (
                self.min_gallery_reviews - outcomes_for_galleries_reviews_new.shape[0]
        )
        if remaining_gallery_reviews > 0:
            outcomes_for_galleries_reviews = pd.concat(
                [
                    outcomes_for_galleries_reviews_new,
                    outcomes_for_galleries_reviews_old.iloc[:remaining_gallery_reviews],
                ],
                axis=0,
            )
        else:
            outcomes_for_galleries_reviews = outcomes_for_galleries_reviews_new[
                                             :self.min_gallery_reviews
                                             ]

        return outcomes_for_articles_recipes, outcomes_for_galleries_reviews


    def _get_recs_based_odd_hour(self, outcomes_for_galleries_reviews_old,
                                 outcomes_for_articles_recipes_new,
                                 outcomes_for_articles_recipes_old
                                 ):
        """

        :param outcomes_for_galleries_reviews_old:
        :param outcomes_for_articles_recipes_new:
        :param outcomes_for_articles_recipes_old:
        :return: old galleries, new articles
        """
        self.logger.info("Odd Hour Calculations")
        outcomes_for_galleries_reviews = outcomes_for_galleries_reviews_old[
                                         :self.min_gallery_reviews
                                         ]

        remaining_articles_recipes = (
                self.min_articles_recipes - outcomes_for_articles_recipes_new.shape[0]
        )
        if remaining_articles_recipes > 0:
            outcomes_for_articles_recipes = pd.concat(
                [
                    outcomes_for_articles_recipes_new,
                    outcomes_for_articles_recipes_old.iloc[:remaining_articles_recipes],
                ],
                axis=0,
            )
        else:
            outcomes_for_articles_recipes = outcomes_for_articles_recipes_new[
                                            :self.min_articles_recipes
                                            ]

        return outcomes_for_articles_recipes, outcomes_for_galleries_reviews


    def _get_outcomes(self, prediction_hour):

        """

        :param prediction_hour: current prediction hour
        :return: recommendations.
        """

        (outcomes_for_articles_recipes_new,
         outcomes_for_articles_recipes_old,
         outcomes_for_galleries_reviews_new,
         outcomes_for_galleries_reviews_old) = self._filter_new_old_articles()

        self.logger.info(
            f"Old articles/recipes count is: {outcomes_for_articles_recipes_old.shape[0]}"
        )
        self.logger.info(
            f"New articles/recipes count is: {outcomes_for_articles_recipes_new.shape[0]}"
        )
        self.logger.info(
            f"Old galleries/reviews count is: {outcomes_for_galleries_reviews_old.shape[0]}"
        )
        self.logger.info(
            f"New galleries/reviews count is: {outcomes_for_galleries_reviews_new.shape[0]}"
        )

        # Alternate hours, (new article old gallery) or (old article new gallery)

        if prediction_hour % 2 == 0:
            # old article(0), new gallery(1)
            self.logger.info("Even prediction hour, old article(0), new gallery(1)")
            (outcomes_for_articles_recipes,
             outcomes_for_galleries_reviews) = self._get_recs_based_even_hour(
                outcomes_for_articles_recipes_old,
                outcomes_for_galleries_reviews_new,
                outcomes_for_galleries_reviews_old
            )
        else:
            # new article(1), old gallery(0)
            self.logger.info("Odd prediction hour, new article(1), old gallery(0)")
            (outcomes_for_articles_recipes,
             outcomes_for_galleries_reviews) = self._get_recs_based_odd_hour(
                outcomes_for_galleries_reviews_old,
                outcomes_for_articles_recipes_new,
                outcomes_for_articles_recipes_old
                )

        self.logger.info(
            f"Checking for article/recipe recency {outcomes_for_articles_recipes.content_recency_when_accessed}"
        )
        self.logger.info(
            f"Checking for gallery/review recency {outcomes_for_galleries_reviews.content_recency_when_accessed}"
        )

        return outcomes_for_articles_recipes, outcomes_for_galleries_reviews


    def get_recommendations(self, **kwargs):

        """

        :param kwargs: prediction_hour
        :return: recommendations
        """

        outcomes_for_articles_recipes,outcomes_for_galleries_reviews = self._get_outcomes(kwargs['prediction_hour'])
        return pd.concat(
            [outcomes_for_articles_recipes, outcomes_for_galleries_reviews], axis=0
        )


class WIRED_RECOMMENDATIONS(BRAND_RECOMMENDATIONS):

    def __init__(self,outcomes, min_articles_recipes, min_gallery_reviews):
        super(WIRED_RECOMMENDATIONS, self).__init__(outcomes, min_articles_recipes, min_gallery_reviews)
        self.outcomes = outcomes
        self.min_articles_recipes = min_articles_recipes
        self.min_gallery_reviews = min_gallery_reviews

    def _filter_article_by_channel(self, value, channel_type):
        """

        :param value: gear/science/backchannel value
        :param channel_type: gear/science/backchannel
        :return: boolean/ custom_outcome
        """
        outcomes = self.outcomes
        if value == 0:
            channel_present = False
            self.logger.info("No {} content needed".format(channel_type))
            custom_outcomes = outcomes.loc[outcomes["channel"].str.lower() != channel_type, :]
            self.logger.info(custom_outcomes.shape)
            self.logger.info("**" * 25)
        else:
            channel_present = True
            custom_outcomes = outcomes.loc[
                              outcomes["channel"].str.lower() == channel_type, :
                              ].iloc[:value]

        return channel_present, custom_outcomes

    def _filter_article_gallery(self, wired_priority_outcomes):
        """

        :param wired_priority_outcomes:
        :return: article/gallery articles
        """
        check_df = wired_priority_outcomes.groupby("content_type").count()[["cid"]]
        check_df = check_df.rename(columns={"cid": "post_counts"}).reset_index()

        wired_priority_articles = check_df.loc[
            check_df["content_type"] == "0", "post_counts"
        ].values
        wired_priority_articles_value = (
            wired_priority_articles[0] if wired_priority_articles.size > 0 else 0
        )

        wired_priority_galleries = check_df.loc[
            check_df["content_type"] == "1", "post_counts"
        ].values
        wired_priority_galleries_value = (
            wired_priority_galleries[0] if wired_priority_galleries.size > 0 else 0
        )

        min_articles_recipes = (
            (self.min_articles_recipes - wired_priority_articles_value)
            if (self.min_articles_recipes - wired_priority_articles_value) > 0
            else 0
        )
        min_gallery_reviews = (
            (self.min_gallery_reviews - wired_priority_galleries_value)
            if (self.min_gallery_reviews - wired_priority_galleries_value) > 0
            else 0
        )
        return min_articles_recipes, min_gallery_reviews

    def get_recommendations(self, **kwargs):
        """

        :param kwargs: prediciton hour and custom inputs
        :return: recommendations
        """

        wired_priority_outcomes_present = False # setting default as false

        priority_outcomes_gear_flag, priority_outcomes_gear_df = self._filter_article_by_channel(
            kwargs["remaining_wired_gear_value"], "gear")
        priority_outcomes_science_flag, priority_outcomes_science_df = self._filter_article_by_channel(
            kwargs["remaining_wired_science_value"], "science")
        priority_outcomes_backchannel_flag, priority_outcomes_backchannel_df = self._filter_article_by_channel(
            kwargs["remaining_wired_backchannel_value"], "backchannel")

        wired_priority_outcomes = pd.DataFrame(columns=self.outcomes.columns)

        wired_priority_outcomes = pd.concat([wired_priority_outcomes, priority_outcomes_gear_df], axis=0) if priority_outcomes_gear_flag else wired_priority_outcomes
        wired_priority_outcomes = pd.concat([wired_priority_outcomes, priority_outcomes_science_df], axis=0) if priority_outcomes_science_flag else wired_priority_outcomes
        wired_priority_outcomes = pd.concat([wired_priority_outcomes, priority_outcomes_backchannel_df], axis=0) if priority_outcomes_backchannel_flag else wired_priority_outcomes


        if wired_priority_outcomes.shape[0] > 0:
            wired_priority_outcomes_present = True
            (self.min_articles_recipes,
             self.min_gallery_reviews) = self._filter_article_gallery(wired_priority_outcomes)

        super().__init__(self.outcomes,self.min_articles_recipes, self.min_gallery_reviews)
        outcomes_for_articles_recipes,outcomes_for_galleries_reviews = super()._get_outcomes(kwargs["prediction_hour"])
        if wired_priority_outcomes_present:
            return pd.concat(
                        [
                            wired_priority_outcomes,
                            outcomes_for_articles_recipes,
                            outcomes_for_galleries_reviews,
                        ],
                        axis=0,
                    )
        else:
            return pd.concat(
                        [
                            outcomes_for_articles_recipes,
                            outcomes_for_galleries_reviews,
                        ],
                        axis=0,
                    )


