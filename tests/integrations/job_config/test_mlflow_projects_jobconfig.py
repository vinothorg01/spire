from datetime import date

from spire.framework.workflows import Workflow, WorkflowStages
from spire.framework.workflows.job_config import (
    MLFlowProjectTrainingJobConfig,
    MLFlowProjectJobDefinition,
)


def test_get_jobs_for_workflows(session):
    wf1 = Workflow("spire_test_1", description="")
    definition = MLFlowProjectJobDefinition(
        uri="", entry_point="", experiment_id="0", backend_config={}
    )

    default_config = MLFlowProjectTrainingJobConfig(
        name="mlflow_config", definition=definition
    )

    wf1.job_configs[WorkflowStages.TRAINING.value] = default_config

    session.add(wf1)
    session.commit()

    wfs = list(session.query(Workflow).all())

    jobs = Workflow.get_jobs_for_workflows(wfs, WorkflowStages.TRAINING, date.today())

    assert len(jobs) == 1
    assert isinstance(jobs[0], MLFlowProjectTrainingJobConfig.get_job_class())
