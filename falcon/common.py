"""
common module is used for common functionalities throughout the project
"""

import logging.config
import os
import numpy as np
import pendulum
import requests
import yaml
import json
from sqlalchemy import Table, select

try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources as pkg_resources

CURR_UNIXTIME = pendulum.now(tz="UTC").int_timestamp


def read_config(settings_name=None):
    """
    Read brand config for the brand names

    Args:
        settings_name: If settings_name is a brand, then its the brand_config
        brand_config_path:

    Returns:

    """
    try:
        with pkg_resources.path("falcon", "settings.yaml") as pkg_path:
            package_path = pkg_path
        with open(package_path, "r") as open_pkg:
            file_text = open_pkg.read()
        brand_config = yaml.safe_load(file_text)
        return brand_config[settings_name] if settings_name else brand_config
    except Exception as yaml_exception:
        raise yaml_exception("YAML File not loaded, Run the code from correct folder !!!")


def read_brand_exclusions():
    """
    Read brand exclusions for every brand
    """
    try:
        with pkg_resources.path("falcon", "brand_exclusions.yaml") as pkg_path:
            package_path = pkg_path
        with open(package_path, "r") as open_pkg:
            file_text = open_pkg.read()
        brand_exclusions = yaml.safe_load(file_text)
        return brand_exclusions
    except Exception as yaml_exception:
        print(yaml_exception)


def setup_logging(default_path="logging.yaml", default_level=logging.INFO):
    """

    Args:
        default_path:
        default_level:

    Returns:

    """
    path = default_path
    if os.path.exists(path):
        with open(path, "rt") as open_pkg:
            config = yaml.safe_load(open_pkg.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def dir_files(path):
    """

    Args:
        path:

    Returns:

    """
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file

def get_unique_brandnames_source(brand_config, source):
    """
    Get the unique brand name based on the data source

    :param brand_config: complete brand configs from postgres across all brands
    :param source: datasource aleph_k2d/ga/sparrow
    :return: list of unique brand names on that particular datasource
    """
    return list(set([x['brand_alias'][source] for x in brand_config]))


def unshorten_url(url):
    """
    Get shortened url
    Args:
        url: The url that is to be shortened
    """

    try:
        session = requests.Session()  # so connections are recycled
        headers = {}
        headers[
            "User-Agent"
        ] = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
        resp = session.head(url, allow_redirects=True, timeout=15, headers=headers)
        return resp.url
    except Exception as url_exception:
        print(f"*** Error in unshorten url function {url_exception} for url {url} ***\n")
        return ""


def clean_urls(url: str) -> (str):
    """
    Get clean urls
    Args:
        url:
    """

    exceptions_list = ["/amp", "/all", "/1"]
    try:
        print(f"Clean url gets the url link {url}")
        url = unshorten_url(url)
        if url == "":
            return ""
        url = url.replace("http://", "https://")
        for exception in exceptions_list:
            if url.endswith(exception):
                url = url.replace(exception, "")
        return (
            url.strip("/")
            .split("?")[0]
            .strip("/")
            .split("#")[0]
            .strip("/")
            .split("%")[0]
            .strip("/")
            .split("mbid=social_facebook")[0]
            .strip("/")
        )
    except Exception as clean_url_exception:
        print(f"Clean urls exception is {clean_url_exception}, return will be empty string")
        return ""


def clean_urls_multiprocessing(zip_list):
    """
    Args:
        zip_list
    """
    # print(type(zip_list))
    if not isinstance(zip_list, tuple):
        raise Exception(
            "Make sure that the arugment is sent in the form of a tuple, put the arguments in parentheses"
        )
    url, *rest = zip_list
    exceptions_list = ["/amp", "/all", "/1"]
    try:
        url = unshorten_url(url)
        if url == "":
            return ("", *rest)
        url = url.replace("http://", "https://")
        for exception in exceptions_list:
            if url.endswith(exception):
                url = url.replace(exception, "")
        return (
            url.strip("/")
            .split("?")[0]
            .strip("/")
            .split("#")[0]
            .strip("/")
            .split("%")[0]
            .strip("/")
            .split("mbid=social_facebook")[0]
            .strip("/"),
            *rest,
        )
    except Exception as url_mp_exception:
        print(
            f"Clean urls multiprocessing exception is {url_mp_exception}, return will be empty string with tuple elements"
        )
        return ("", *rest)


def get_min_positive_after_diff(coll_hfac, hfac):
    """
    Args:
        coll_hfac:
        hfac:
    """
    hfac_np_array = np.array(coll_hfac)
    if np.sum(hfac_np_array >= 0) > 0:
        b = int(hfac) - hfac_np_array
        c = b >= 0
        if np.sum(c) > 0:
            return int(np.min(b[np.where(c)]))
        else:  # all date in the future (ERROR)
            return -999  # int(np.min(b))
    else:  # all dates in the past
        return int(hfac) + abs(int(np.max(hfac_np_array)))


def write_db_as_delta(to_write_df, brand_name, platform_name, path, hfac, schema=None, spark = None):
    """
    Args:
        to_write_df:
        brand_name:
        platform_name:
        path:
        hfac:
        schema:
        spark:
    """
    if to_write_df.shape[0] > 0:
        to_write_df.loc[:, "brand"] = brand_name
        to_write_df.loc[:, "platform_name"] = platform_name
        to_write_df.loc[:, "hfac"] = hfac
        to_write_df["hfac"] = to_write_df["hfac"].astype(np.int64)
        data_pq_bucket = path  # falcon_config[filetype]["s3_bucket"][mode]
        print(data_pq_bucket)

        if schema:
            to_write_spark_df = spark.createDataFrame(to_write_df, schema=schema)
        else:
            to_write_spark_df = spark.createDataFrame(to_write_df)

        to_write_spark_df.repartition(1).write.partitionBy("brand", "platform_name", "hfac").format("delta").mode(
            "append").save(data_pq_bucket)

        return to_write_spark_df, to_write_spark_df.schema
