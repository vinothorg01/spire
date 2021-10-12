import pytest
import sqlalchemy
from spire.framework.workflows.job_config import (
    JobConfig,
    SpireNotebookJobConfig,
)
from spire.framework.workflows import Workflow, WorkflowStages
from tests.data.cluster_config import generic_config as sample_config


def test_databricks_notebook_config(session):
    config = SpireNotebookJobConfig(
        name="test_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )

    session.add(config)
    session.commit()
    loaded_config = (
        session.query(JobConfig).filter(JobConfig.name == "test_config").one()
    )
    # Make sure polymorphism works
    assert isinstance(loaded_config, SpireNotebookJobConfig)
    w = Workflow("test", "test")
    jobs = loaded_config.get_jobs(workflows=[w])

    assert isinstance(jobs[0], SpireNotebookJobConfig.get_job_class())
    assert len(jobs) == 1


def test_adding_job_configs(session):
    test_wf = Workflow("test", description="")
    default_config = SpireNotebookJobConfig(
        name="default_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )

    session.add(test_wf)
    session.add(default_config)
    session.commit()

    wf = session.query(Workflow).filter(Workflow.name == "test").one()
    config = session.query(JobConfig).filter(JobConfig.name == "default_config").one()

    wf.job_configs[WorkflowStages.TRAINING.value] = config
    wf.job_configs[WorkflowStages.SCORING.value] = config

    session.add(wf)
    session.commit()

    wf = session.query(Workflow).filter(Workflow.name == "test").one()
    assert len(wf.job_configs.keys()) == 2
    assert (
        wf.job_configs[WorkflowStages.TRAINING]
        == wf.job_configs[WorkflowStages.SCORING]
    )


def test_adding_unpersisted_job_config(session):
    test_wf = Workflow("test", description="")
    session.add(test_wf)
    session.commit()

    wf = session.query(Workflow).filter(Workflow.name == "test").one()
    default_config = SpireNotebookJobConfig(
        name="default_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )

    with pytest.raises(sqlalchemy.orm.exc.NoResultFound):
        session.query(JobConfig).filter(JobConfig.name == "default_config").one()

    wf.job_configs[WorkflowStages.TRAINING.value] = default_config
    wf.job_configs[WorkflowStages.SCORING.value] = default_config

    session.add(wf)
    session.commit()

    wf = session.query(Workflow).filter(Workflow.name == "test").one()
    assert len(wf.job_configs.keys()) == 2
    assert (
        wf.job_configs[WorkflowStages.TRAINING]
        == wf.job_configs[WorkflowStages.SCORING]
    )

    config = session.query(JobConfig).filter(JobConfig.name == "default_config").one()
    assert config is not None


def test_list_workflows_using_a_config(session):
    wf1 = Workflow("test", description="")
    wf2 = Workflow("test2", description="")
    wf3 = Workflow("test3", description="")

    default_config = SpireNotebookJobConfig(
        name="default_config",
        definition={
            "new_cluster": sample_config["new_cluster"],
            "notebook_task": {"notebook_path": "/foo/bar"},
        },
    )

    wf1.job_configs[WorkflowStages.TRAINING.value] = default_config
    wf2.job_configs[WorkflowStages.TRAINING.value] = default_config
    wf3.job_configs[WorkflowStages.TRAINING.value] = default_config

    session.add(wf1)
    session.add(wf2)
    session.add(wf3)
    session.commit()

    config = session.query(JobConfig).filter(JobConfig.name == "default_config").one()
    assert len(config.workflows) == 3
    for wf in config.workflows:
        assert isinstance(wf, Workflow)
