import os

from unittest.mock import MagicMock, call

from spire.framework.workflows import Workflow
from spire.framework.workflows.postprocessor import Thresholder

ENVIRONMENT = os.environ.get("DEPLOYMENT_ENV", "test")


def _init_dummy_decile():
    return Thresholder(
        strategy="decile",
        thresholds=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
        buckets=[
            "decile99",
            "decile90",
            "decile80",
            "decile70",
            "decile60",
            "decile50",
            "decile40",
            "decile30",
            "decile20",
            "decile10",
        ],
    )


def _init_dummy_percentile():
    return Thresholder(
        strategy="percentile", thresholds=[0.25], buckets=["high_score", "low_score"]
    )


def _init_dummy_absolute():
    return Thresholder(
        strategy="absolute", thresholds=[0.06], buckets=["high_score", "low_score"]
    )


def test_commit_thresholder():
    # setup
    session = MagicMock()
    wf = Workflow("test", "test_description")
    decile = _init_dummy_decile()
    percentile = _init_dummy_percentile()
    absolute = _init_dummy_absolute()
    wf.append_postprocessor(session, decile)
    wf.append_postprocessor(session, percentile)
    wf.append_postprocessor(session, absolute)

    # exercise
    session.add_all([decile, percentile, absolute])
    session.commit()

    # assert
    session.add_all.assert_has_calls([call(wf.postprocessor)])
    session.commit.assert_called_once()


def test_thresholder_identity():
    # setup
    session = MagicMock()
    wf = Workflow("test", "test_description")
    decile = _init_dummy_decile()
    percentile = _init_dummy_percentile()
    absolute = _init_dummy_absolute()
    wf.append_postprocessor(session, decile)
    wf.append_postprocessor(session, percentile)
    wf.append_postprocessor(session, absolute)

    # exercise
    dec_type = wf.postprocessor[0].type
    per_type = wf.postprocessor[1].type
    abs_type = wf.postprocessor[2].type

    # assert
    dec_type == "Thresholder"
    per_type == "Thresholder"
    abs_type == "Thresholder"


def test_add_thresholder():
    # setup
    session = MagicMock()
    wf = Workflow("test", "test_description")
    decile = _init_dummy_decile()
    percentile = _init_dummy_percentile()
    absolute = _init_dummy_absolute()

    # exercise
    wf.append_postprocessor(session, decile)
    wf.append_postprocessor(session, percentile)
    wf.append_postprocessor(session, absolute)

    # assert
    assert len(wf.postprocessor) == 3


def test_remove_thresholder():
    # setup
    session = MagicMock()
    wf = Workflow("test", "test_description")
    expected_postprocessor_count = 2
    decile = _init_dummy_decile()
    percentile = _init_dummy_percentile()
    absolute = _init_dummy_absolute()
    wf.append_postprocessor(session, decile)
    wf.append_postprocessor(session, percentile)
    wf.append_postprocessor(session, absolute)

    # exercise
    wf.remove_postprocessor(decile)

    # assert
    assert len(wf.postprocessor) == expected_postprocessor_count
