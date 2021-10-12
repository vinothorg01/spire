import uuid
import traceback
from datetime import date
from pyspark.sql import SparkSession
from sqlalchemy.orm import Session
from .postprocessor import Postprocessor
from spire.framework.score.utils import write_scores
from spire.framework.workflows.workflow_stages import WorkflowStages
from spire.framework.execution import JobStatuses
from spire.utils.logger import get_logger
from spire.utils.spark_helpers import read_with_partition_col

logger = get_logger()


class MLFlowScoreProcessor(Postprocessor):
    __mapper_args__ = {"polymorphic_identity": "MLFlowScoreProcessor"}
    required_input_columns = ["xid", "score", "date", "tid", "decile"]

    def __init__(self):
        self.id = uuid.uuid4()

    def _get_input_df(self, spark, run_date):
        input_uri = self.workflow.job_configs[WorkflowStages.SCORING].get_output_uri(
            self.workflow, run_date
        )

        logger.info(f"Reading input dataframe from {input_uri}")
        df = read_with_partition_col(input_uri)
        return df

    def _validate_input(self, df):
        missing_columns = set()
        for c in self.required_input_columns:
            if c not in df.columns:
                missing_columns.add(c)

        if len(missing_columns) > 0:
            raise ValueError(f"Missing columns {missing_columns} from input dataframe.")

    def postprocess(
        self,
        run_date: date,
        session: Session,
        spark: SparkSession,
    ):
        response = {
            "workflow_id": str(self.workflow.id),
            "stage": str(WorkflowStages.POSTPROCESSING),
            "status": str(JobStatuses.SUCCESS),
            "arg_date": run_date.strftime("%Y-%m-%d"),
            "info": {
                "stage_type": self.__mapper_args__["polymorphic_identity"],
            },
            "error": None,
        }
        try:
            logger.info(f"Start processing {self} on {run_date}")
            df = self._get_input_df(spark, run_date)
            self._validate_input(df)

            # select only relevant columns
            df = df.select(*self.required_input_columns)

            write_scores(run_date, self.workflow, df)

        except Exception as e:
            logger.error(f"Failed to process {self} on {run_date}", exc_info=True)
            tb = traceback.format_exc()
            response["traceback"] = tb
            response["error"] = e
            response["status"] = str(JobStatuses.FAILED)

        return response

    def __str__(self) -> str:
        return f"<{self.__class__.__name__}(workflow_id={self.workflow.id})>"
