import time
import datetime
import traceback

import numpy as np

from pyspark.ml.feature import VectorAssembler
from hyperopt import hp

from kalos.models import LogisticRegression
from kalos.tuner import HyperparameterTuner

from spire.framework.train import constants
from spire.utils import s3
from spire.utils.logger import get_logger
from spire.config import config


logger = get_logger()


def assemble_training_key(bucket, key, workflow_id):
    """
                               __..--.._
      .....              .--~  .....  `.
    .":    "`-..  .    .' ..-'"    :". `
    ` `._ ` _.'`"(     `-"'`._ ' _.' '
         ~~~      `.          ~~~
                  .'
                 /
                (
                 ^---'
    """
    return "{}/workflow_id={}".format(s3.assemble_path(bucket, key), workflow_id)


def train_workflows(spark, workflows, date, session):
    responses = []
    for workflow in workflows:
        responses.append(train(spark, workflow, date, session))
    return responses


def train(spark, workflow, date, session):
    """
    Trains a model to be associated with the trait_id/date pair provided in
    workflows using the data with training_df.  Currently always trains a
    logistic regression model, but in future will read instructions for which
    model to train from train_recipe (name temporary) similar to what is
    currently done for assembly.
    TODO: Refactor to allow training recipes.

    ARGS:
        spark : spark.SparkSession
            Loaded Spark session.
        workflow : spire.framework.Workflow
            The Spire Workflow object to train.
        date : datetime.date
            The date for which to train the workflow.
        session : sqlalchemy session
            The session the workflow belongs to.


    RETURNS:
        model : kalos.engine.model
            A trained Kalos model associated with the provided trait_id/date.
        response : dictionary
            A dictionary containing information about model training.
    """

    try:
        logger.info(f"Begin training workflow {str(workflow)}")
        df = load_training_data(spark, workflow, date)
        run_id, exp_id, metrics, warnings = _train(df, workflow, date)
        status = "success"
        execution_time = datetime.datetime.fromtimestamp(time.time())
        response = make_training_response(
            workflow,
            date,
            status,
            run_id=run_id,
            exp_id=exp_id,
            execution_time=execution_time,
            stats=metrics,
            warnings=warnings,
            error=None,
            tb=None,
        )
        logger.info(f"Training succeeded for workflow {str(workflow)}", extra=response)
    except Exception as e:
        status = "failed_on_train"
        tb = traceback.format_exc()
        execution_time = datetime.datetime.fromtimestamp(time.time())
        response = make_training_response(
            workflow,
            date,
            status,
            run_id=None,
            exp_id=None,
            execution_time=execution_time,
            stats=None,
            warnings=None,
            error=e,
            tb=tb,
        )

        logger.error(f"Training failed for workflow {str(workflow)}", extra=response)

    workflow.commit_history(response, session)
    return response


def _train(df, workflow, date):
    df_train, df_val, df_test = df.randomSplit([0.6, 0.2, 0.2], constants.SEED)
    va = VectorAssembler(
        inputCols=[col for col in df.columns if col[:8] == "feature_"],
        outputCol="features",
    )

    df_train_assembled = va.transform(df_train)
    df_val_assembled = va.transform(df_val)
    df_test_assembled = va.transform(df_test)

    lr = LogisticRegression()
    bs = HyperparameterTuner(
        lr,
        strategy="Bayesian",
        objective_metric_name="roc_auc",
        parameter_range={
            "regParam": hp.loguniform("regParam", -6 * np.log(10), -2 * np.log(10)),
            "elasticNetParam": hp.uniform("elasticNetParam", 0.0, 1.0),
        },
        max_evals=4,
        parameter_option="hyperopt",
    )

    lr = bs.tune(
        df_train=df_train_assembled, df_val=df_val_assembled, df_test=df_test_assembled
    )

    metrics = {}

    train_metrics = lr.evaluate(df_train_assembled, ["roc_auc", "balanced_accuracy"])
    train_metrics = {
        "train_{}".format(key): value for key, value in train_metrics.items()
    }
    metrics.update(train_metrics)

    test_metrics = lr.evaluate(df_test_assembled, ["roc_auc", "balanced_accuracy"])
    test_metrics = {"test_{}".format(key): value for key, value in test_metrics.items()}
    metrics.update(test_metrics)

    lr.log(
        "/Production/spire/experiments/workflow_id={}".format(workflow.id),
        date.strftime("%Y-%m-%d"),
    )
    run_id = lr.history.history["mlflow"]["run_id"]
    exp_id = lr.history.history["mlflow"]["exp_id"]
    warnings = {}

    if abs(
        metrics["train_{}".format(constants.DEFAULT_METRIC)]
        - metrics["test_{}".format(constants.DEFAULT_METRIC)]
        > 0.05
    ):
        warnings[
            "overfit_warning"
        ] = """\
        train_{metric} - test_{metric} = {value}." The model has probably overfit.
        """.format(
            metric=constants.DEFAULT_METRIC,
            value=(
                metrics["train_{}".format(constants.DEFAULT_METRIC)]
                - metrics["test_{}".format(constants.DEFAULT_METRIC)]
            ),
        )

    if metrics["test_{}".format(constants.DEFAULT_METRIC)] < 0.6:
        warnings[
            "poor_model_warning"
        ] = "test_{metric} = {value} < .6." "  The model is poor.".format(
            metric=constants.DEFAULT_METRIC,
            value=metrics["test_{}".format(constants.DEFAULT_METRIC)],
        )
    return run_id, exp_id, metrics, warnings


def load_training_data(spark, workflow, date):
    df = spark.read.format(constants.TRAIN_FORMAT).load(
        assemble_training_key(config.SPIRE_ENVIRON, constants.TRAIN_KEY, workflow.id)
    )
    df = df.fillna(0)
    return df


def make_training_response(
    workflow,
    date,
    status,
    run_id=None,
    exp_id=None,
    execution_time=None,
    stats=None,
    warnings=None,
    error=None,
    tb=None,
):
    """
    Helper function that assembles the training response dictionary.

    ARGS:
        workflow : spire.framework.Workflow
            Spire workflow object the response is built for.
        date : date
            The date for which the training was performed.
        status : str
            Either 'success' or 'failed_on_train'.
        execution_time : datetime.datetime
            Time at which the process ran
        run_id : str
            The mlflow run_id associated with the model.
        exp_id : str
            The mlflow experiment_id associated with the model.
        stats : dictionary
            A dictionary of metrics from the model training/evaluation,
            e.g. test_auc.
        warnings : dictionary
            Any warning that were raised by the model training process.
        error : str
            If the training failed, the raised exception, otherwise None.
        tb : str
            If the training failed, the traceback associated with the
            raised exception, otherwise None.
    RETURNS:
        response : dictionary
            A logable response object associated with the run.
    """

    warnings = warnings or {}
    stats = stats or {}
    info = {}
    if error is not None:
        err = str(error).replace("'", "''")
        tb = tb.replace("'", "''")
    else:
        err = None
    info["name"] = workflow.name
    info["description"] = workflow.description.replace("'", "''")
    info["mlflow_run_id"] = run_id
    info["mlflow_exp_id"] = exp_id
    arg_date = datetime.datetime.combine(date, datetime.datetime.min.time())
    response = {
        "workflow_id": str(workflow.id),
        "stage": "training",
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
