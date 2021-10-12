from click.testing import CliRunner
from cli.workflows.schedules_cli import schedules
import tempfile
import os
import csv
from csv import writer
from spire.framework.workflows.workflow import Workflow


def test_get_schedule(session, wf_def):
    # setup
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    runner = CliRunner()
    args = ["get", "--id", str(wf.id)]

    result = runner.invoke(schedules, args)
    print("test_int_get_schedule")
    print(result.output)
    assert "assembly" in result.output
    assert "scoring" in result.output
    assert "training" in result.output
    assert "logic: cron" in result.output
    assert "logic: scheduled_stages" in result.output
    assert result.exit_code == 0


def test_get_schedule_stage(session, wf_def):
    # setup
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    runner = CliRunner()

    # execution
    stage = "assembly"
    args = ["get", "--id", str(wf.id), "--stage", stage]

    result = runner.invoke(schedules, args)
    print("test_int_get_schedule_stage")
    print(result.output)
    assert "assembly" in result.output
    assert "logic: cron" in result.output
    assert result.exit_code == 0


def test_get_schedule_wrong_option(session, wf_def):
    # setup
    runner = CliRunner()
    args = ["get", "--i", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1"]

    result = runner.invoke(schedules, args)
    assert result.exit_code == 2


def test_get_schedule_wrong_command(session, wf_def):
    # setup
    runner = CliRunner()
    args = ["gt", "--id", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1"]

    result = runner.invoke(schedules, args)
    print("test_get_init_schedule_wrong_command")
    print(result.output)
    assert result.exit_code == 2


def test_schedule_batch_update(session, wf_def):
    # setup
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    runner = CliRunner()

    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_update.csv")
    fieldnames = [
        "id",
        "name",
        "description",
        "enabled",
        "vendor",
        "source",
        "groups",
        "dataset",
        "trait_id",
        "schedule_start_date",
        "schedule",
    ]
    with open(filepath, "w", newline="") as csvfile:
        dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        dict_writer.writeheader()
    with open(filepath, "a", newline="") as write_obj:
        csv_writer = writer(write_obj)
        csv_writer.writerow(
            [
                str(wf.id),
                "spire_foo_test",
                "test > test",
                "False",
                "adobe",
                "",
                "",
                "",
                "",
                "2021-01-01",
                "",
            ]
        )
    args = ["batch-update", "--filepath", filepath]

    # execution
    result = runner.invoke(schedules, args)
    # assert
    assert "assembly" in result.output
    assert "scoring" in result.output
    assert "training" in result.output
    assert wf.schedule is not None
    assert result.exit_code == 0


def test_schedule_batch_update_wrong_file(session, wf_def):
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_update.csv")
    args = ["batch-update", "--filepath", filepath]

    # execution
    result = runner.invoke(schedules, args)
    # assert
    assert result.exit_code == 1


def test_schedule_batch_update_wrong_option(session, wf_def):
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_update.csv")
    args = ["batch-update", "--filepat", filepath]

    # execution
    result = runner.invoke(schedules, args)
    # assert
    assert result.exit_code == 2


def test_schedule_batch_update_wrong_command(session, wf_def):
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_update.csv")
    args = ["batch-updat", "--filepath", filepath]

    # execution
    result = runner.invoke(schedules, args)
    # assert
    assert result.exit_code == 2
