import re
import time
import traceback
import datetime
from functools import reduce

import pyspark.sql.functions as F

from spire.utils import s3
from spire.framework.assemble import constants
from spire.config import config


def assemble(features_df, targets_df, workflows, date, session):
    data = [
        load_target(targets_df, workflow, target=workflow.name)
        for workflow in workflows
    ]
    output_dfs = [datum[0] for datum in data]
    load_responses = [datum[1] for datum in data]
    outputs_df = reduce(
        lambda x_df, y_df: join_ignore_null(
            x_df, y_df, on=constants.ID_VAR, how="outer"
        ),
        output_dfs,
    )
    if outputs_df is None:
        return [
            make_and_log_binary_response(workflow, date, load_response, None, session)
            for workflow, load_response in zip(workflows, load_responses)
        ]

    output_df2 = features_df.join(outputs_df, on=constants.ID_VAR, how="inner").cache()
    write_responses = [
        write_dataset(output_df2, workflow, workflow.name) for workflow in workflows
    ]
    return [
        make_and_log_binary_response(
            workflow, date, load_response, write_response, session
        )
        for workflow, load_response, write_response in zip(
            workflows, load_responses, write_responses
        )
    ]


def load_target(targets_df, workflow, target="target"):
    try:
        dataset = workflow.dataset
        output_df = (
            targets_df.select(constants.ID_VAR, dataset.target(name=target))
            .filter(F.col(target).isNotNull())
            .groupby(constants.ID_VAR)
            .agg(F.max(target).alias(target))
            .cache()
        )
        load_counts = dataset.requirements.check(output_df, target_col=target)
        return output_df, load_counts
    except Exception as e:
        response = {"error": str(e), "traceback": traceback.format_exc()}
        return None, response


def write_dataset(output_df, workflow, target="target"):
    try:
        dataset = workflow.dataset
        nearly_there = (
            output_df.select(
                constants.ID_VAR,
                *[col for col in output_df.columns if col[:8] == "feature_"],
                target
            )
            .withColumnRenamed(target, "target")
            .filter(F.col("target").isNotNull())
        )
        output_df2, write_counts = dataset.requirements.meet(nearly_there)

        output_df2.write.format(constants.OUT_FORMAT).mode("overwrite").save(
            "{}/workflow_id={}".format(
                s3.assemble_path(config.SPIRE_ENVIRON, constants.OUT_KEY), workflow.id
            )
        )
        return write_counts
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}


def join_ignore_null(x_df, y_df, on=None, how="inner"):
    if x_df and y_df:
        return x_df.join(y_df, on=on, how=how)
    if not (x_df or y_df):
        return None
    return x_df or y_df


def make_and_log_binary_response(
    workflow, date, load_response, write_response, session
):
    stats = {}
    warnings = {}
    error = None
    tb = None
    status = "success"
    execution_time = datetime.datetime.fromtimestamp(time.time())
    if "error" in load_response.keys():
        error = load_response["error"]
        try:
            tb = load_response["traceback"]
        except KeyError:
            warnings[
                "load_response_failure"
            ] = "Error logged but no traceback reported."
        status = "failed_on_load_target"
        response = _mbr_helper(
            workflow.id,
            date,
            status,
            stats,
            workflow.name,
            workflow.description,
            error=error,
            tb=tb,
            warnings=warnings,
            execution_time=execution_time,
        )
        workflow.commit_history(response, session)
        return response
    else:
        try:
            stats["total_positives"] = load_response["positive"]
            stats["total_negatives"] = load_response["negative"]
        except KeyError:
            warnings[
                "load_response_failure"
            ] = "No error logged but no stats not reported."

    if "error" in write_response.keys():
        error = write_response["error"]
        try:
            tb = write_response["traceback"]
        except KeyError:
            warnings[
                "join_or_write_response_failure"
            ] = "Error logged but no traceback reported."
        status = "failed_on_join_or_write"
    else:
        try:
            stats["final_positives"] = write_response["positive"]
            stats["final_negatives"] = write_response["negative"]
        except KeyError:
            warnings[
                "join_or_write_response_failure"
            ] = "No error logged but no stats not reported."

    if "warnings" in load_response.keys():
        warnings.update(load_response["warnings"])
    if "warnings" in write_response.keys():
        warnings.update(write_response["warnings"])

    if "warnings" in load_response.keys():
        warnings.update(load_response["warnings"])
    if "warnings" in write_response.keys():
        warnings.update(write_response["warnings"])

    response = _mbr_helper(
        workflow.id,
        date,
        status,
        stats,
        workflow.name,
        workflow.description,
        error=error,
        tb=tb,
        warnings=warnings,
        execution_time=execution_time,
    )
    workflow.commit_history(response, session)
    return response


def _mbr_helper(
    id,
    date,
    status,
    stats,
    name,
    description,
    error=None,
    tb=None,
    warnings=None,
    execution_time=None,
):
    warnings = warnings or {}
    info = {}
    if error is not None:
        err = str(error).replace("'", "''")
        tb = tb.replace("'", "''")
    else:
        err = None
    info["name"] = re.sub(r"(\W)", "", name)
    info["description"] = re.sub(r"(\W)", "", description)
    arg_date = datetime.datetime.combine(date, datetime.datetime.min.time())
    response = {
        "workflow_id": str(id),
        "stage": "assembly",
        "status": status,
        "error": err,
        "traceback": tb,
        "arg_date": arg_date,
        "execution_date": execution_time,
        "stats": stats,
        "warnings": warnings,
        "info": info,
    }
    return response
