import datetime
from typing import Tuple, Dict, Any, List, Set, Union, TYPE_CHECKING

from unittest.mock import MagicMock

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.ml.feature import VectorAssembler, QuantileDiscretizer

from sqlalchemy.orm.session import Session

from kalos.models.spark import LogisticRegression  # type: ignore
from spire.utils.spark_helpers import spark_session
from spire.utils.logger import get_logger
from spire.framework.score import constants, errors
from spire.framework.score.utils import (
    get_history,
    write_scores,
)
from spire.framework.score.passthrough import _score_passthrough

if TYPE_CHECKING:
    # TODO(Max): Cyclic import resolution
    from spire.framework.workflows import Workflow
    from spire.framework.workflows.history import History


logger = get_logger()


@spark_session
def _score(
    workflow: "Workflow",
    date: datetime.date,
    session: Session,
    spark: SparkSession = None,
) -> Tuple[str, str, Dict[str, Any], Dict[str, Any]]:
    print("scoring {}".format(workflow.id))
    if workflow.is_proxy:
        # NOTE(Max): This is a temporary measure
        run_id, experiment_id, stats = _score_passthrough(date, workflow, spark=spark)
        return run_id, experiment_id, stats
    features = load_features(spark, date)
    if features.rdd.isEmpty():
        message = (
            f"Features for {date} is an empty dataframe. Have features been written?"
        )
        raise errors.MissingFeaturesError(message)
    scores, run_id, experiment_id = kalos_inference(date, features, workflow, session)
    scores = transform_scores(scores, date, workflow)
    write_scores(date, workflow, scores)
    stats = make_stats_report(scores)
    return run_id, experiment_id, stats


def create_scores(
    lr: Union[LogisticRegression, MagicMock], features: DataFrame
) -> DataFrame:
    """
    Derives predictions (scores) on the feature set from the model (lr).
    temp_score adds random noise (epsilon) which facilitates discretization of deciles
    or other thresholds in cases where we have a large number of identical scores
    """
    if not isinstance(lr, (LogisticRegression, MagicMock)):
        raise Exception("Unable to load model.")
    scores = lr.predict(features).select(
        constants.FEATURES_ID_VAR, F.col("probability1").alias("score")
    )
    epsilon = 0.0001
    return scores.withColumn(
        "temp_score", (F.col("score") + epsilon * F.rand()).astype(FloatType())
    )


def load_features(spark: SparkSession, date: datetime.date) -> DataFrame:
    features = (
        spark.read.format(constants.DELTA_FORMAT)
        .load(f"s3a://{constants.FEATURE_BUCKET}/{constants.FEATURE_KEY}")
        .where(F.col(constants.FEATURES_DATE_VAR) == date)
        .fillna(0)
    )
    return features


def load_model(
    run_id: str,
    experiment_id: str,
) -> Tuple[Any, Any, Any]:
    lr = LogisticRegression()
    lr.load(run_id, experiment_id=experiment_id)
    return lr, run_id, experiment_id


def prep_features(features: DataFrame, session: Session = None) -> DataFrame:
    """
    Vectorizes the scoring features and transforms them
    (or for tests, mocks them instead)
    TODO(Max): Given the fixtures and integrations we now have, there is no longer a
    need to mock the VectorAssembler anymore, and tests should be changed accordingly.
    """
    if isinstance(session, MagicMock):
        va = MagicMock()
    else:
        va = VectorAssembler(  # type: ignore
            inputCols=[col for col in features.columns if col[:8] == "feature_"],
            outputCol="features",
        )
    return va.transform(features)


def get_mlflow_ids(last_successful_train: "History") -> Set[str]:
    if not last_successful_train:
        message = (
            "The query to obtain last_successful_train returned nothing."
            "It is highly likely that this model was not trained OR it"
            " does not have an info dict."
        )
        raise errors.MissingHistoryError(message)
    run_id = last_successful_train.info.get("mlflow_run_id")
    experiment_id = last_successful_train.info.get("mlflow_exp_id")
    if run_id is None or experiment_id is None:
        message = (
            "This training history report is missing an MLFlow run_id or"
            " experiment_id. Has this model been trained?"
        )
        raise errors.MLFlowDependenceError(message)
    return (run_id, experiment_id)


def kalos_inference(
    date: datetime.date,
    features: DataFrame,
    workflow: "Workflow",
    session: Session = None,
) -> Tuple[DataFrame, Dict[str, Any], str, str]:
    last_successful_train = get_history(workflow, date, session)
    run_id, experiment_id = get_mlflow_ids(last_successful_train)
    features = prep_features(features, session)
    lr, run_id, experiment_id = load_model(run_id, experiment_id)
    scores = create_scores(lr, features)
    return scores, run_id, experiment_id


def transform_scores(
    df: DataFrame, date: datetime.datetime, workflow: "Workflow"
) -> DataFrame:
    qd = QuantileDiscretizer(numBuckets=10, inputCol="temp_score", outputCol="decile")
    df = qd.fit(df).transform(df)
    df.drop("temp_score")
    df = df.select(
        F.col(constants.FEATURES_ID_VAR).astype(StringType()).alias("xid"),
        F.col("score").astype(FloatType()).alias("score"),
        (F.lit(10) - F.col("decile")).cast(IntegerType()).alias("decile"),
    )
    return df.withColumn("date", F.lit(date)).withColumn(
        "tid", F.lit(workflow.trait.trait_id)
    )


def make_scores_stats(df: DataFrame) -> DataFrame:
    return (
        df.groupby("decile")
        .agg(
            F.count("*").alias("count"),
            F.max("score").alias("max_score"),
            F.min("score").alias("min_score"),
            F.avg("score").alias("avg_score"),
        )
        .orderBy("decile")
    )


def make_stats_report(df: DataFrame) -> Dict[str, List]:
    df = make_scores_stats(df)
    deciles = list(
        map(
            lambda x: {
                "decile": x["decile"],
                "count": x["count"],
                "max_score": x["max_score"],
                "min_score": x["min_score"],
                "avg_score": x["avg_score"],
            },
            df.collect(),
        )
    )
    return {"decile_stats": deciles}
