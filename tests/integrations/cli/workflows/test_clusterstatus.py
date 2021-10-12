from click.testing import CliRunner
from cli.workflows.clusterstatuses_cli import clusterstatuses
from spire.framework.workflows.workflow import Workflow


def test_cluster_status(session, wf_def):
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    runner = CliRunner()
    args = ["get", "--id", str(wf.id)]
    result = runner.invoke(clusterstatuses, args)
    assert "stage: assembly_status" in result.output
    assert "stage: training_status" in result.output
    assert "stage: scoring_status" in result.output

    assert result.exit_code == 0


def test_get_cluster_status_stage(session, wf_def):
    # setup
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    runner = CliRunner()
    stage = "assembly"
    args = ["get", "--id", str(wf.id), "--stage", stage]

    result = runner.invoke(clusterstatuses, args)
    assert "stage: assembly_status" in result.output
    assert "stage: training_status" not in result.output
    assert "stage: scoring_status" not in result.output
    assert result.exit_code == 0


def test_get_cluster_status_wrong_data():
    # setup
    runner = CliRunner()
    stage = "assembly"
    args = ["get", "--id", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1", "--stage", stage]
    result = runner.invoke(clusterstatuses, args)
    assert result.exit_code == 1


def test_get_cluster_status_wrong_option():
    # setup
    runner = CliRunner()
    stage = "assembly"
    args = ["get", "--i", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1", "--sta", stage]
    result = runner.invoke(clusterstatuses, args)
    assert result.exit_code == 2


def test_get_cluster_status_wrong_command():
    # setup
    runner = CliRunner()
    args = ["ge", "--id", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1"]
    result = runner.invoke(clusterstatuses, args)
    assert result.exit_code == 2
