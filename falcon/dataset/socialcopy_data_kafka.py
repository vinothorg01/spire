# -*- coding: utf-8 -*-
import logging
import os
from sqlalchemy import MetaData, Table, insert, select, update
from falcon.utils.datetime_utils import DateTimeUtils
from falcon.database.db_utils import DBUtils

logger = logging.getLogger(__name__)


def update_socialcopy_db_data(
    db_config,
    social_platform_id,
    brand_id,
    social_partner_object,
    socialcopy_config,
    social_platform_name,
    social_partner,
):

    # Function Variables
    db_connection = db_config["connection"]
    socialcopy_db = db_config["socialcopy_db"]
    first_time_update = False
    db_utils = DBUtils.init_with_db_config(db_config)

    try:
        ts_epoch = DateTimeUtils.get_now_epoch_time(tz="UTC")

        # Query 1 :  Read the fbsocialcopy_last_update_time from db, if it exists
        last_recorded_ts = db_utils.get_socialcopy_last_update_time(socialcopy_db, social_platform_id, brand_id)
        if last_recorded_ts is not None:
            logger.info(
                f"From {DateTimeUtils.convert_epochtime_todatetime_str(last_recorded_ts)} ({last_recorded_ts}) to {DateTimeUtils.convert_epochtime_todatetime_str(ts_epoch)} ({ts_epoch})!!!"
            )
            prediction_start_datetime = last_recorded_ts
            prediction_end_datetime = ts_epoch
        else:
            first_time_update = True

            # Done to get the complete datetime string
            min_time_var_name = f"{social_platform_name}_min_date"
            brand_min_time = DateTimeUtils.convert_datetimestr_epochtime(
                socialcopy_config[min_time_var_name], tz="UTC"
            )
            logger.info(
                f"From {DateTimeUtils.convert_epochtime_todatetime_str(brand_min_time)} ({brand_min_time}) to {DateTimeUtils.convert_epochtime_todatetime_str(ts_epoch)} ({ts_epoch})!!!"
            )
            prediction_start_datetime = brand_min_time
            prediction_end_datetime = ts_epoch

        # Access Social Data first
        if social_partner == "socialflow":
            (
                social_brand_content_df,
                non_social_brand_content_df,
            ) = social_partner_object.get_socialflow_posted_data(
                posted_data_hours=None,
                start_time=prediction_start_datetime,
                end_time=prediction_end_datetime,
            )
            if social_brand_content_df.shape[0] > 0:
                with db_connection.begin() as trans:
                    if first_time_update:
                        # Query 2 : Insert the timestamp in the database for the first run
                        rowcount = db_utils.insert_socialcopy_last_update_time(socialcopy_db, social_platform_id, brand_id, ts_epoch)
                        assert rowcount == 1
                    else:
                        # Query 3 : Update the most recent datetime for any brand
                        rowcount = db_utils.update_socialcopy_last_update_time(socialcopy_db, social_platform_id, brand_id, ts_epoch)
                        assert rowcount == 1
            return (
                social_brand_content_df,
                non_social_brand_content_df,
                first_time_update,
            )
        else:
            raise Exception(
                "Implementation of this social platform is not yet available!"
            )

    except Exception as e:
        logging.exception(f"Check the error !!!, {e}")
