import copy
import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    BooleanType,
)
import pyspark.sql.functions as F
from spire.utils.spark_helpers import init_spark
from spire.framework.workflows import Workflow


def generate_stage_report(
    stage, env="production", arg_date=None, as_of=None, spark=None
):
    if spark is None:
        spark = init_spark()
    report_schema = StructType(
        [
            StructField("workflow_id", StringType(), False),
            StructField("enabled", BooleanType(), True),
            StructField("trait_id", IntegerType(), True),
            StructField("name", StringType(), False),
            StructField("ui_name", StringType(), True),
            StructField("stage", StringType(), False),
            StructField("arg_date", DateType(), True),
            StructField("execution_date", DateType(), True),
            StructField("status", StringType(), False),
            StructField("info", StringType(), False),
            StructField("stats", StringType(), False),
            StructField("warnings", StringType(), False),
            StructField("error", StringType(), True),
            StructField("traceback", StringType(), True),
        ]
    )
    reports = copy.deepcopy(
        Workflow.get_reports(stage, env, arg_date=arg_date, as_of=as_of)
    )

    for report in reports:
        for key in ["info", "stats", "warnings"]:
            report[key] = json.dumps(report.pop(key))
        report["workflow_id"] = report.pop("workflow_id").__str__()
        if "trait_id" not in report.keys():
            report["trait_id"] = None
    return spark.createDataFrame(reports, report_schema)


def generate_training_report(env="production", arg_date=None, as_of=None, spark=None):
    assembly_df = generate_stage_report(
        "assembly", env=env, arg_date=arg_date, as_of=as_of, spark=spark
    ).drop("stage")
    training_df = generate_stage_report(
        "training", env=env, arg_date=arg_date, as_of=as_of, spark=spark
    ).drop("stage")
    join_cols = ["workflow_id", "trait_id", "name", "ui_name"]
    assembly_df = assembly_df.select(
        *join_cols,
        *[
            F.col(col).alias("assembly_{}".format(col))
            for col in assembly_df.columns
            if col not in join_cols
        ],
    )
    training_df = training_df.select(
        *join_cols,
        *[
            F.col(col).alias("training_{}".format(col))
            for col in training_df.columns
            if col not in join_cols
        ],
    )
    report_df = assembly_df.join(training_df, on=join_cols, how="outer")
    return report_df
