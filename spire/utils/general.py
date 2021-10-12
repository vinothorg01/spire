import ruamel
from ruamel import yaml
import datetime
import traceback
from collections import defaultdict
from typing import List, Any
from spire.utils.logger import get_logger

logger = get_logger(__name__)


def chunk_into_groups(total_group, group_size) -> List[List[Any]]:
    result = []
    for i in range(0, len(total_group), group_size):
        result.append(total_group[i : i + group_size])
    return result


def run_with_retry(script, func, timeout, args={}, max_retries=3):
    num_retries = 0
    while True:
        try:
            return yaml.load(func(script, timeout, args), Loader=ruamel.yaml.Loader)
        except Exception as e:
            if num_retries >= max_retries:
                return {"error": str(e), "traceback": traceback.format_exc()}
            else:
                print("Retrying after error: ")
                print(e)
                num_retries += 1


def assert_datetime_argument(date):
    if isinstance(date, datetime.datetime):
        return datetime.datetime.combine(date.date(), datetime.datetime.min.time())
    elif isinstance(date, datetime.date):
        return datetime.datetime.combine(date, datetime.datetime.min.time())
    else:
        raise Exception("target_date must be date or datetime.")


def singleton(cls):
    instance = [None]

    def wrapper(*args, **kwargs):
        if instance[0] is None:
            instance[0] = cls(*args, **kwargs)
        return instance[0]

    return wrapper


def group_by_vendor(workflows):
    groups = defaultdict(list)
    for wf in workflows:
        vendor = wf.vendor
        if not vendor:
            logger.warning(f"workflow {str(wf.id)} has invalid " f"name {wf.name}")
        groups[vendor].append(wf)
    return groups


def get_docker_image(spire_image, spire_version):
    return f"ghcr.io/condenast/{spire_image}:{spire_version}"
