# -*- coding: utf-8 -*-


class Feature_Selection:
    """A class to save the features used for different modeling approaches"""

    def __init__(self):
        return

    def get_features_logistic_prod(self, df, data_for="train"):
        pred_cols = [
            c
            for c in df.columns
            if ((c.find("prev") > -1) & (c.find("facebook") == -1))
        ]  # + ['hours_from_anchor_point']
        final_cols = pred_cols + [
            "content_type",
            "content_recency_when_accessed",
            "total_pageviews_next_6hrs",
            "final_social_posted",
        ]
        if data_for == "train":
            df = df[final_cols]
            df = df.drop_duplicates(
                keep="first",
                subset=[
                    "total_pageviews_prev_3hrs_2hrs",
                    "total_pageviews_prev_2hrs_1hrs",
                    "total_pageviews_prev_1hrs",
                    "anon_visits_prev_3hrs_2hrs",
                    "anon_visits_prev_2hrs_1hrs",
                    "anon_visits_prev_1hrs",
                    "social_visits_prev_3hrs_2hrs",
                    "social_visits_prev_2hrs_1hrs",
                    "social_visits_prev_1hrs",
                    "content_type",
                    "content_recency_when_accessed",
                ],
            )
            df = df.reset_index(drop=True)
            return df, final_cols
        else:
            return df, final_cols
