# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from sqlalchemy import MetaData, Table, delete, select, and_, text
from falcon.database.db_utils import DBUtils

from falcon.utils.datetime_utils import DateTimeUtils


def processing_hold_df_forlabels(hold_df, **kwargs):
    """
    Processes Hold DF, returns the sanitized versions
    """
    hold_df = hold_df.copy()
    s_obj = kwargs.get("s_obj")
    db_connection = kwargs.get("db_config")["connection"]
    falcon_later_db = kwargs.get("db_config")["later_db"]
    falcon_never_db = kwargs.get("db_config")["never_db"]
    brand_id = kwargs.get("brand_id")
    social_platform_id = kwargs.get("social_platform_id")
    mode = kwargs.get("mode")
    db_utils = DBUtils.init_with_db_config(kwargs.get("db_config"))

    falcon_labels = s_obj.falcon_labels

    for label in list(falcon_labels.keys()):
        if label == "falcon_later":
            print(f"Check for the label {label}")
            recommend_after_hours = falcon_labels[label]["recommend_after_hours"]

            print("Checking for the later labels in hold_df !!!")
            later_hold_df = hold_df.loc[hold_df.labels.str.contains(label), :].copy()
            hold_df = hold_df.loc[~hold_df.labels.str.contains(label), :].copy()
            if isinstance(later_hold_df, pd.DataFrame) and later_hold_df.shape[0] > 0:
                print(
                    f"Later label found in hold_df, total elements {later_hold_df.shape[0]} !!!"
                )
                later_hold_df["recycle_ts_epoch"] = pd.to_datetime(
                    later_hold_df.created_ts_epoch, utc=True, unit="s"
                ) + pd.Timedelta(
                    seconds=(s_obj.shelf_life + recommend_after_hours) * 60 * 60
                )
                later_hold_df["recycle_ts_epoch"] = (
                    later_hold_df["recycle_ts_epoch"].astype(np.int64) // 10 ** 9
                )
                later_hold_df = later_hold_df.rename(
                    columns={"labels": "falcon_labels"}
                )
                later_hold_df["social_platform_id"] = social_platform_id
                later_hold_df["brand_id"] = brand_id
                later_hold_df.drop("score", axis=1, inplace=True)

                # Check if the content_item_id exist in the later_hold_df
                later_hold_df_listdict = later_hold_df.to_dict(orient="records")

                for element in later_hold_df_listdict:
                    later_select_predicate = falcon_later_db.columns.content_item_id == element["content_item_id"]
                    result = db_utils.select_from_table(falcon_later_db, later_select_predicate)
                    if result.rowcount == 0:
                        print(
                            "Content Item Id to be added to the db, not already present"
                        )
                        rowcount = db_utils.insert_element_to_table(falcon_later_db, element)
                        assert rowcount == 1
                        if mode == "prod":
                            assert (
                                s_obj.deletequeue([element["content_item_id"]])[
                                    "status"
                                ]
                                == 200
                            )
                            print(
                                f"element added to the db and removed from queue with content_item_id "
                                f"{element['content_item_id']}\n"
                            )
                        else:
                            print(
                                "element recorded in the dev database for falcon later, but not removed"
                            )
                    else:
                        print(
                            f"element already found in db, ignored  {element['content_item_id']}\n"
                        )
            else:
                print("NO FALCON LATER UPDATES !!!\n")

        if label == "falcon_never":
            print(f"Check for the label {label}")

            print(
                "Never hold df created from the hold_df to find the falcon never labels !!!"
            )
            never_hold_df = hold_df.loc[hold_df.labels.str.contains(label), :].copy()
            hold_df = hold_df.loc[~hold_df.labels.str.contains(label), :].copy()

            if isinstance(never_hold_df, pd.DataFrame) and never_hold_df.shape[0] > 0:
                print("never_hold_df has elements in the dataframe, shown below :")
                never_hold_df = never_hold_df.rename(
                    columns={"labels": "falcon_labels"}
                )
                never_hold_df["social_platform_id"] = social_platform_id
                never_hold_df["brand_id"] = brand_id
                never_hold_df.drop("score", axis=1, inplace=True)

                # Check if the content_item_id exist in the never_hold_df
                never_hold_df_listdict = never_hold_df.to_dict(orient="records")

                for element in never_hold_df_listdict:
                    never_select_predicate = falcon_never_db.columns.content_item_id == element["content_item_id"]
                    result = db_utils.select_from_table(falcon_never_db, never_select_predicate)
                    if result.rowcount == 0:
                        print(
                            "Content Item Id to be added to the db, not already present"
                        )
                        rowcount = db_utils.insert_element_to_table(falcon_never_db, element)
                        assert rowcount == 1
                        if mode == "prod":
                            assert (
                                s_obj.deletequeue([element["content_item_id"]])[
                                    "status"
                                ]
                                == 200
                            )
                            print(
                                f"element added to the db and removed from queue with content_item_id "
                                f"{element['content_item_id']}\n"
                            )
                        else:
                            print(
                                "element added to falcon never database but not removed from the queue in dev mode"
                            )
                    else:
                        print(
                            f"element already found in db, ignored  {element['content_item_id']}\n"
                        )
            else:
                print("NO FALCON NEVER UPDATES !!!\n")

    hold_brand_df, hold_nonbrand_df = s_obj.sanitize_content_df(
        hold_df, on_column="link"
    )

    return (hold_brand_df, hold_nonbrand_df)


def recycle_falcon_later_df_load_never_df(**kwargs):
    """
    If any value in the recycle_ts_EST > current epoch time, then just remove it from the database

    Steps:
    1. Find the current epoch time
    2. Delete values from falcon later database where recycle time is more than or equal to the current epoch time
    3. Return all the values from the falcon later database as the output of this function
    4. Also return all the values from the falcon never database as the output of this function

    """
    s_obj = kwargs.get("s_obj")
    db_config = kwargs.get("db_config")
    social_platform_id = kwargs.get("social_platform_id")
    brand_id = kwargs.get("brand_id")

    # Step 1
    current_epoch_time = DateTimeUtils.get_now_epoch_time()

    # Step 2
    db_connection = db_config["connection"]
    falcon_later_db = db_config["later_db"]
    falcon_never_db = db_config["never_db"]
    db_utils = DBUtils.init_with_db_config(kwargs.get("db_config"))

    later_delete_predicate = and_(text("brand_id='{}'".format(brand_id)),
         text("social_platform_id='{}'".format(social_platform_id)),
              text("recycle_ts_epoch <  '{}'".format(current_epoch_time)))
    result_proxy1 = db_utils.delete_from_table(falcon_later_db, later_delete_predicate)
    print(
        f"Number of rows recycled from falcon later database are {result_proxy1.rowcount}"
    )

    # Step 3
    later_select_predicate = and_(text("brand_id='{}'".format(brand_id)),
         text("social_platform_id='{}'".format(social_platform_id)))
    result_proxy2 = db_utils.select_from_table(falcon_later_db, later_select_predicate)
    print(
        f"Number of rows returned from the falcon later database are {result_proxy2.rowcount}"
    )

    # Step 4
    never_predicate = text("brand_id='{}'".format(brand_id))
    result_proxy3 = db_utils.select_from_table(falcon_never_db, predicate=never_predicate)
    print(
        f"Number of rows returned from the falcon never database are {result_proxy3.rowcount}"
    )

    falcon_later_df = pd.DataFrame(
        result_proxy2.fetchall(), columns=result_proxy2.keys()
    )
    falcon_never_df = pd.DataFrame(
        result_proxy3.fetchall(), columns=result_proxy3.keys()
    )

    # Check if it returns a dataframe
    assert isinstance(falcon_later_df, pd.DataFrame)
    assert isinstance(falcon_never_df, pd.DataFrame)

    if "id" in falcon_later_df.columns:
        falcon_later_df = falcon_later_df.drop("id", axis=1)
    if "id" in falcon_never_df.columns:
        falcon_never_df = falcon_never_df.drop("id", axis=1)

    falcon_later_brand_df, falcon_later_nonbrand_df = s_obj.sanitize_content_df(
        falcon_later_df, on_column="link"
    )
    falcon_never_brand_df, falcon_never_nonbrand_df = s_obj.sanitize_content_df(
        falcon_never_df, on_column="link"
    )

    return (falcon_later_brand_df, falcon_later_nonbrand_df), (
        falcon_never_brand_df,
        falcon_never_nonbrand_df,
    )


def add_copilot_ids_to_brand_dfs(brand_df, multiple_urls_vals_df):
    """
    Args:
        brands_df:
        multiple_urls_vals_df:
    """
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