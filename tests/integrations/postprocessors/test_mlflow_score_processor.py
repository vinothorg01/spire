import pytest
import pandas as pd
from pathlib import Path
from datetime import date
from unittest.mock import patch
from spire.framework.workflows import Workflow
from spire.framework.workflows.postprocessor.mlflow_score_processor import (
    MLFlowScoreProcessor,
)


class TestMLFlowScoreProcessor:
    @pytest.fixture()
    def processor_instance(self, wf_def, session):
        wf = Workflow.create_default_workflow(**wf_def, session=session)
        session.add(wf)
        wf.update_trait_id(session, 1)
        session.commit()

        proc = MLFlowScoreProcessor()
        wf.postprocessor.append(proc)
        session.commit()

        return proc

    @pytest.fixture()
    def input_data(self, spark):
        df = spark.createDataFrame(
            pd.DataFrame(
                {
                    "xid": [1, 2, 3],
                    "score": [1, 1, 1],
                    "decile": [1, 1, 1],
                    "date": [date.today(), date.today(), date.today()],
                    "tid": [1, 1, 2],
                }
            )
        )
        return df

    def test_add_processor_to_workflow(self, processor_instance, session):

        assert len(processor_instance.workflow.postprocessor) == 1
        assert processor_instance.workflow.postprocessor[0] == processor_instance

        result = session.query(MLFlowScoreProcessor).first()
        assert result == processor_instance

    def test_write(self, processor_instance, input_data, session, spark, tmpdir):
        """Test that processor write score into existing directory with
        partition correctly.
        """

        with patch.object(processor_instance, "_get_input_df") as get_input_df, patch(
            "spire.framework.score.scoring.constants.DELTA_OUTPUT_URI",
            str(tmpdir),
        ), patch(
            "spire.framework.score.scoring.constants.PARQUET_OUTPUT_URI",
            str(tmpdir / "parquet"),
        ):
            # create existing directory
            Path(tmpdir / "date=2020-01-01" / "tid=123123").mkdir(parents=True)
            get_input_df.return_value = input_data

            response = processor_instance.postprocess(
                date.today(), session, spark=spark
            )
            # for debugging when test fails
            if "traceback" in response:
                print(response["traceback"])
            assert response["error"] is None

            assert (tmpdir / f"date={date.today()}").exists()
            assert (tmpdir / f"date={date.today()}" / "tid=1").exists()
            assert (tmpdir / f"date={date.today()}" / "tid=2").exists()

    def test_validate_data(self, processor_instance, spark, session, input_data):
        with patch.object(processor_instance, "_get_input_df") as get_input_df:
            get_input_df.return_value = input_data.drop("tid")
            response = processor_instance.postprocess(
                date.today(), session, spark=spark
            )
            assert response["error"] is not None
