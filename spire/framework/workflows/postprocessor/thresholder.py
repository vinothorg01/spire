import time
import datetime
import traceback
import re
import uuid
from pyspark.sql.session import SparkSession

from sqlalchemy import String, Float, Column, ForeignKey
from sqlalchemy.orm.session import Session
from sqlalchemy.types import ARRAY
from sqlalchemy.dialects.postgresql import UUID
from . import Postprocessor

from spire.framework.postprocess import constants
from spire.utils import s3
from spire.config import config

from pyspark.ml.feature import Bucketizer
import pyspark.sql.functions as F
from pyspark.sql.window import Window


class Thresholder(Postprocessor):
    __tablename__ = "thresholds"
    id = Column(
        UUID(as_uuid=True),
        ForeignKey(Postprocessor.__table__.c.id, ondelete="CASCADE"),
        primary_key=True,
    )
    strategy = Column(String(100))
    thresholds = Column(ARRAY(Float))
    buckets = Column(ARRAY(String))

    def __init__(self, strategy, thresholds, buckets, enabled=False):
        self.id = uuid.uuid4()
        self.thresholds = thresholds
        self.strategy = strategy
        self.buckets = buckets
        self.enabled = enabled

    __mapper_args__ = {"polymorphic_identity": "Thresholder"}

    def postprocess(self, date: datetime.date, session: Session, spark: SparkSession):
        """
        This function thresholds scores for the trait specified in workflows,
        for all postprocessors of type 'threshold', for all users
        active on the specified date, and writes the result out to a
        location specified in constants.

        ARGS:
            self: a postprocessor workflow subclass of type threshold.
            df: the dataframe of scores for a given trait on a given date.
            date: the date of the scores being thresholded.
            session : sqlalchemy session the workflow belongs to.
        RETURNS:
            response: dictionary: a loggable response for the run.
        """

        try:
            df = self.__load_df(date, spark)
            self.validate()
            df = self.__threshold_preprocess(df)
            df = self.__make_threshold_buckets(df)
            df = self.__make_threshold_table(df)
            self.__write_thresholds(df, date)
            status = "success"
            response = self.__make_thresholding_response(
                date,
                status,
                execution_time=datetime.datetime.fromtimestamp(time.time()),
                error=None,
                tb=None,
            )
        except Exception as e:
            tb = traceback.format_exc()
            status = "failed_on_threshold"
            response = self.__make_thresholding_response(
                date,
                status,
                execution_time=datetime.datetime.fromtimestamp(time.time()),
                error=e,
                tb=tb,
            )
        self.workflow.commit_history(response, session)
        return response

    def validate(self):
        strat_dict = {
            "decile": self.__decile_validate,
            "percentile": self.__percentile_validate,
            "absolute": self.__absolute_validate,
        }
        if self.strategy not in strat_dict:
            raise NotImplementedError(
                "{} is not a supported strategy for Threshold".format(self.strategy)
            )
        return strat_dict[self.strategy]()

    def to_dict(self):
        output = {}
        output["postprocessor_id"] = self.id
        output["type"] = "Thresholder"
        output["workflow_id"] = self.workflow_id
        if hasattr(self, "strategy"):
            output["strategy"] = self.strategy
        if hasattr(self, "thresholds"):
            output["thresholds"] = self.thresholds
        if hasattr(self, "buckets"):
            output["buckets"] = self.thresholds
        return output

    def __load_df(self, date, spark):
        score_dict = {
            "s3_path": s3.assemble_path("cn-spire", constants.SCORE_KEY),
            "date_var": constants.DATE_VAR,
            "date": date.strftime("%Y-%m-%d"),
            "sid_var": constants.SID_VAR,
            "workflow_trait_id": self.workflow.trait.trait_id,
        }

        scorepath = (
            "{s3_path}/{date_var}={date}/"
            "{sid_var}={workflow_trait_id}".format(**score_dict)
        )

        df = spark.read.format(constants.IN_FORMAT).load(scorepath).cache()
        return df

    def __threshold_preprocess(self, df):
        w = Window.orderBy(F.col("score").desc())
        df = df.withColumn(
            "raw_percentile",
            F.when(F.col("score").isNull(), F.lit(None)).otherwise(
                F.percent_rank().over(w)
            ),
        )
        return df

    def __make_threshold_table(self, df):
        for i in range(len(self.buckets)):
            df = df.withColumn(
                "bucket", F.when(df.bucket == i, self.buckets[i]).otherwise(df.bucket)
            )

        df = (
            df.groupBy("bucket")
            .agg(
                F.min("score").alias("min_score"),
                F.max("score").alias("max_score"),
                F.avg("score").alias("avg_score"),
            )
            .withColumn("strategy", F.lit(self.strategy))
        )

        df = df.select(
            "strategy", "bucket", "min_score", "max_score", "avg_score"
        ).orderBy(df.avg_score.desc())
        return df

    def __make_threshold_buckets(self, df):
        input_lookup = {
            "decile": "raw_percentile",
            "percentile": "raw_percentile",
            "absolute": "score",
        }
        bucket_input = input_lookup[self.strategy]
        splits = [-float("Inf")] + self.thresholds + [float("Inf")]
        bucketer = Bucketizer(splits=splits, inputCol=bucket_input, outputCol="bucket")
        return bucketer.transform(df)

    def __write_thresholds(self, df, date):
        thresh_dict = {
            "s3_path": s3.assemble_path(
                config.SPIRE_ENVIRON, constants.THRESHOLD_OUT_KEY
            ),
            "date_var": constants.DATE_VAR,
            "date": date.strftime("%Y-%m-%d"),
            "sid_var": constants.SID_VAR,
            "workflow_trait_id": self.workflow.trait.trait_id,
            "pid_var": constants.PID_VAR,
            "postprocess_id": self.id,
        }

        df.write.format(constants.OUT_FORMAT).mode("overwrite").save(
            "{s3_path}/{date_var}={date}/{sid_var}={workflow_trait_id}/"
            "{pid_var}={postprocess_id}".format(**thresh_dict)
        )

    def __make_thresholding_response(
        self, date, status, execution_time=None, error=None, tb=None
    ):
        """
        Helper function to produce the response object that gets logged after
        a thresholding run.

        ARGS:
            self: a postprocessor workflow subclass of type threshold.
            date: the date of the scores being thresholded.
            status : string
                the status of the run (success/failure).
            execution_time : timestamp
                time that the model was run
            error : exception
                the error encountered by the run if it failed.
            tb : string
                the traceback of the error encountered by the run if it
                failed.
        RETURNS:
            response: dictionary: a logable response for the run.
        """
        info = {}
        if error is not None:
            err = str(error).replace("'", "''")
            tb = tb.replace("'", "''")
        else:
            err = None
        info["name"] = re.sub(r"(\W)", "", self.workflow.name)
        info["description"] = re.sub(r"(\W)", "", self.workflow.description)
        info["postprocess_id"] = str(self.id)

        arg_date = datetime.datetime.combine(date, datetime.datetime.min.time())
        response = {
            "workflow_id": str(self.workflow.id),
            "stage": "thresholding",
            "status": status,
            "error": err,
            "traceback": tb,
            "arg_date": arg_date,
            "execution_date": execution_time,
            "stats": {},
            "warnings": {},
            "info": info,
        }
        return response

    def __decile_validate(self):
        assert len(self.thresholds) == 10 - 1, (
            "decile thresholds require a list of nine thresholds"
            " to produce ten buckets"
        )

    def __percentile_validate(self):
        assert isinstance(self.thresholds, list), (
            "percentile thresholds require a list, even if it contains"
            " only one percentile threshold value"
        )

    def __absolute_validate(self):
        assert isinstance(self.thresholds, list), (
            "absolute thresholds require a list, even if"
            " it contains only one absolute threshold value"
        )
