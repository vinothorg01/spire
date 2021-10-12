import datetime
from spire.framework.execution import JobStatuses
from spire.framework.workflows import History, WorkflowStages
from spire.framework.workflows.workflow import Workflow
from spire.framework.workflows.constants import (
    DEFAULT_ASSEMBLY_CONFIG_NAME,
    DEFAULT_TRAINING_CONFIG_NAME,
    DEFAULT_SCORING_CONFIG_NAME,
)
from spire.framework.workflows.job_config import JobConfig
from spire.framework.workflows.cluster.assembly_status import AssemblyStatus
from spire.framework.workflows.cluster.training_status import TrainingStatus
from spire.framework.workflows.cluster.scoring_status import ScoringStatus
from spire.framework.workflows.schedule import CronSchedule


def assert_all_schedules(wf):
    assert all(
        [type(schedule) == CronSchedule for schedule in wf.schedule.schedules.values()]
    )


def test_create_default_workflow(session, wf_def):
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    assembly_config = (
        session.query(JobConfig)
        .filter(JobConfig.name == DEFAULT_ASSEMBLY_CONFIG_NAME)
        .one()
    )
    training_config = (
        session.query(JobConfig)
        .filter(JobConfig.name == DEFAULT_TRAINING_CONFIG_NAME)
        .one()
    )
    scoring_config = (
        session.query(JobConfig)
        .filter(JobConfig.name == DEFAULT_SCORING_CONFIG_NAME)
        .one()
    )
    assert wf.trait is None
    assert wf.dataset
    assert wf.schedule
    assert len(wf.cluster_status) == 3
    assert len(wf.job_configs) == 3
    assert wf.job_configs["assembly"] == assembly_config
    assert wf.job_configs["training"] == training_config
    assert wf.job_configs["scoring"] == scoring_config


def test_delete_workflow(session, wf_def):
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    assert wf.name == wf_def["name"]
    session.delete(wf)
    session.commit()
    assert session.query(Workflow).filter_by(name=wf_def["name"]).count() == 0


def test_get_workflow_by_id(session, wf_def):
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    wf_id = wf.id
    wfById = Workflow.get_by_id(session, wf_id)
    assert wfById.id == wf.id


def test_get_workflow_by_ids(session):
    wf_def_1 = {
        "name": "test_get_workflow_by_id_1",
        "description": "test > test_get_workflow_by_id_1",
        "vendor": "adobe",
        "groups": ["16501880"],
    }
    wf_def_2 = {
        "name": "test_get_workflow_by_id_2",
        "description": "test > test_get_workflow_by_id_2",
        "vendor": "adobe",
        "groups": ["16501881"],
    }
    wf1 = Workflow.create_default_workflow(**wf_def_1, session=session)
    wf2 = Workflow.create_default_workflow(**wf_def_2, session=session)
    session.add_all([wf1, wf2])
    wf_ids = [wf1.id, wf2.id]
    wfs_by_ids = Workflow.get_by_ids(session, wf_ids)
    assert len(wfs_by_ids) == 2


def test_create_default_workflow_metadata(session, wf_def):
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    assert wf.name == wf_def["name"]
    assert wf.description == wf_def["description"]


def test_create_default_workflow_clusterstatus(session, wf_def):
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    assert len(wf.cluster_status) == 3
    assert any([type(status) == AssemblyStatus for status in wf.cluster_status])
    assert any([type(status) == TrainingStatus for status in wf.cluster_status])
    assert any([type(status) == ScoringStatus for status in wf.cluster_status])


def test_create_default_workflow_schedule(session, wf_def):
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    assert len(wf.schedule.schedules) == 3
    assert "assembly" in wf.schedule.schedules.keys()
    assert "training" in wf.schedule.schedules.keys()
    assert "scoring" in wf.schedule.schedules.keys()
    assert_all_schedules(wf)


def test_create_default_workflow_add_workflow_to_balanced_schedules(session, wf_def):
    balance_def = wf_def
    balance_def.pop("schedule")
    wf = Workflow.create_default_workflow(**balance_def, session=session)
    session.add(wf)
    assert len(wf.schedule.schedules) == 3
    assert "assembly" in wf.schedule.schedules.keys()
    assert "training" in wf.schedule.schedules.keys()
    assert "scoring" in wf.schedule.schedules.keys()
    assert_all_schedules(wf)


def test_update_attributes(session, wf_def):
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    assert wf.trait is None
    trait_id = 1
    dataset_def = {
        "family": "binary",
        "data_requirements": {
            "logic": "min_max_provided",
            "min_positive": 100,
            "min_negative": 100,
            "max_positive": 250000,
            "max_negative": 250000,
        },
    }
    schedule_def = {
        "logic": "scheduled_stages",
        "args": {
            "scoring": {
                "logic": "cron",
                "args": {"cron_interval": "0 0 * * *", "start_date": "2020-08-01"},
            },
            "assembly": {
                "logic": "cron",
                "args": {"cron_interval": "0 10 * * 4", "start_date": "2020-08-01"},
            },
            "training": {
                "logic": "cron",
                "args": {"cron_interval": "0 10 * * 4", "start_date": "2020-08-01"},
            },
        },
    }
    history_def = {
        "workflow_id": wf.id,
        "stage": str(WorkflowStages.SCORING),
        "status": str(JobStatuses.SUCCESS),
        "error": None,
        "traceback": None,
        "arg_date": datetime.datetime(2021, 3, 4, 0, 0),
        "execution_date": datetime.datetime(2021, 3, 4, 17, 42, 57, 443521),
        "stats": {
            "total_positives": 5,
            "total_negatives": 5,
            "final_positives": 5,
            "final_negatives": 5,
        },
        "warnings": {},
        "info": {"name": f"{wf.name}", "description": f"{wf.description}"},
    }
    wf.update_trait_id(session, trait_id)
    assert wf.trait.trait_id == 1
    wf.update_dataset(session, dataset_def)
    assert wf.dataset.definition == dataset_def
    wf.update_schedule(session, schedule_def)
    assert wf.schedule.to_dict() == schedule_def
    wf.commit(session)
    session.add(wf)
    old_history = session.query(History).filter(History.workflow_id == wf.id).all()
    assert old_history == []
    wf.commit_history(history_def, session)
    wf.commit(session)
    session.add(wf)
    new_history = session.query(History).filter(History.workflow_id == wf.id).all()
    assert len(new_history) == 1
