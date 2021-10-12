import pytest
import copy
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch
from pyspark.sql import functions as F
from spire.framework.workflows import Workflow, Trait
from spire.framework.score import errors
from spire.framework.score.scoring import (
    get_mlflow_ids,
    transform_scores,
    make_scores_stats,
)
from spire.framework.score.passthrough import get_passthrough_group
from spire.framework.score.utils import write_scores


def test_transform_scores(scores_fixture, scores_tran_fixture, wf_def, date_fixture):
    wf = Workflow(**wf_def)
    trait = Trait(trait_id=1)
    wf.trait = trait
    transformed_scores = transform_scores(scores_fixture, date_fixture, wf)
    # TODO(Max): Adding date and tid to transform_scores is a more recent change
    # Probably should just be added to the fixture, but dropping those columns for now
    transformed_scores = transformed_scores.drop("date", "tid")
    # The fixture was written in such a way that there was rounding, so the output
    # needs to be rounded as well
    transformed_scores = transformed_scores.withColumn(
        "score", F.round(transformed_scores["score"], scale=3)
    )
    num_diff_output_fixture = transformed_scores.exceptAll(scores_tran_fixture).count()
    assert num_diff_output_fixture == 0


def test_make_scores_stats(scores_fixture, scores_stats_fixture, wf_def, date_fixture):
    # The rounding issue in test_transform_scores breaks this test if I try to use the
    # scores_tran_fixture directly
    wf = Workflow(**wf_def)
    trait = Trait(trait_id=1)
    wf.trait = trait
    transformed_scores = transform_scores(scores_fixture, date_fixture, wf)
    scores_stats = make_scores_stats(transformed_scores)
    num_diff_output_fixture = scores_stats.exceptAll(scores_stats_fixture).count()
    assert num_diff_output_fixture == 0


def test_get_passthrough_group_dermstore(wf_proxy_def):
    wf_proxy_def["name"] = "spire_adobe_2864"
    workflow = Workflow(**wf_proxy_def)
    dermstore_group = get_passthrough_group(workflow)
    assert dermstore_group == 17907200


def test_get_passthrough_group_covid(wf_proxy_def):
    wf_proxy_def["name"] = "spire_condenast_2890"
    workflow = Workflow(**wf_proxy_def)
    covid_group = get_passthrough_group(workflow)
    assert covid_group == "covid_readers"


def test_get_passthrough_group_affiliate(wf_proxy_def):
    wf_proxy_def["name"] = "spire_condenast_3443"
    workflow = Workflow(**wf_proxy_def)
    affiliate_group = get_passthrough_group(workflow)
    assert affiliate_group == "affiliate_commerce_clicks"


def test_get_passthrough_group_exception(wf_proxy_def):
    # Regular Workflow
    workflow = Workflow(**wf_proxy_def)
    with pytest.raises(Exception):
        get_passthrough_group(workflow)


def test_write_scores(tmpdir, scores_fixture, wf_def):
    wf = Workflow(**wf_def)
    trait = Trait(trait_id=1)
    wf.trait = trait
    dt = date.today()
    scores_transformed = transform_scores(scores_fixture, dt, wf)
    with patch(
        "spire.framework.score.scoring.constants.DELTA_OUTPUT_URI",
        str(tmpdir / "delta"),
    ), patch(
        "spire.framework.score.scoring.constants.PARQUET_OUTPUT_URI",
        str(tmpdir / "parquet"),
    ):
        Path(tmpdir / "delta" / "date=2020-01-01" / "tid=123123").mkdir(parents=True)
        Path(tmpdir / "parquet" / "date=2020-01-01" / "tid=123123").mkdir(parents=True)

        write_scores(dt, wf, scores_transformed)
        assert (tmpdir / "delta" / f"date={dt}").exists()
        assert (tmpdir / "delta" / f"date={dt}" / f"tid={trait.trait_id}").exists()
        assert (tmpdir / "parquet" / f"date={dt}").exists()
        assert (tmpdir / "parquet" / f"date={dt}" / f"tid={trait.trait_id}").exists()


def test_get_mlflow_ids(last_successful_train_fixture):
    last_successful_train = copy.deepcopy(last_successful_train_fixture)
    # For serialization purposes, the workflow relationship must be mocked in order to
    # use this History.
    last_successful_train.workflow = MagicMock()
    last_successful_train.info = {"mlflow_run_id": 123, "mlflow_exp_id": 456}
    run_id, experiment_id = get_mlflow_ids(last_successful_train)
    assert run_id == last_successful_train.info["mlflow_run_id"]
    assert experiment_id == last_successful_train.info["mlflow_exp_id"]


def test_mlflow_dependence_error(last_successful_train_fixture):
    """
    Test that an MLFlowDependenceError is raised when a History instance lacks a .info
    attribute, or is missing mlflow_run_id or mlflow_exp_id
    """
    last_successful_train = copy.deepcopy(last_successful_train_fixture)
    # For serialization purposes, the workflow relationship must be mocked in order to
    # use this History.
    last_successful_train.workflow = MagicMock()
    # Add .info with fake run_id and exp_id
    last_successful_train.info = {"mlflow_run_id": 123, "mlflow_exp_id": 456}
    no_mlflow_run_id = copy.deepcopy(last_successful_train)
    no_mlflow_run_id.info["mlflow_run_id"] = None
    with pytest.raises(errors.MLFlowDependenceError):
        get_mlflow_ids(no_mlflow_run_id)
    no_mlflow_exp_id = copy.deepcopy(last_successful_train)
    no_mlflow_exp_id.info["mlflow_exp_id"] = None
    with pytest.raises(errors.MLFlowDependenceError):
        get_mlflow_ids(no_mlflow_exp_id)


def test_missing_history_error():
    """
    Test that a MissingHistoryError is raised when a
    training History instance does not exist
    """
    last_successful_train = None
    with pytest.raises(errors.MissingHistoryError):
        get_mlflow_ids(last_successful_train)
