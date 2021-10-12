from click.testing import CliRunner
from cli.workflows.traits_cli import traits
import tempfile
import os
import csv
from csv import writer
from spire.framework.workflows.workflow import Workflow


def test_trait_update_get(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    # update trait
    update_args = ["update", "--id", str(wf.id), "--trait-id", 9999]
    runner.invoke(traits, update_args)
    # execution
    get_args = ["get", "--id", str(wf.id)]
    result = runner.invoke(traits, get_args)
    assert "trait_id: 9999" in result.output
    assert result.exit_code == 0


def test_trait_get_wrong_option():
    runner = CliRunner()
    args = ["get", "--i", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1"]
    result = runner.invoke(traits, args)
    assert result.exit_code == 2


def test_trait_get_wrong_command():
    runner = CliRunner()
    args = ["gt", "--id", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1"]
    result = runner.invoke(traits, args)
    assert result.exit_code == 2


def test_trait_update_wrong_option():
    runner = CliRunner()
    args = ["update", "--i", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1", "--trait-id", 9998]
    result = runner.invoke(traits, args)
    assert result.exit_code == 2


def test_trait_update_wrong_command():
    runner = CliRunner()
    args = ["updat", "--id", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1", "--trait-id", 9998]
    result = runner.invoke(traits, args)
    assert result.exit_code == 2


def test_traits_batch_update(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
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
                wf.name,
                wf.description,
                "True",
                "",
                "",
                "",
                "",
                9991,
                "",
            ]
        )
        csv_writer.writerow(
            [
                str(wf.id),
                wf.name,
                wf.description,
                "True",
                "",
                "",
                "",
                "",
                9992,
                "",
            ]
        )
    args = ["batch-update", "--filepath", filepath]
    # execution
    result = runner.invoke(traits, args)
    # assert
    args = ["get", "--id", str(wf.id)]
    result = runner.invoke(traits, args)
    assert "trait_id: 9992" in result.output
    assert result.exit_code == 0


def test_traits_batch_update_wrong_file():
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_update.csv")
    args = ["batch-update", "--filepath", filepath]
    # execution
    result = runner.invoke(traits, args)
    # assert
    assert result.exit_code == 1


def test_trait_batch_update_wrong_option(session):
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_update.csv")
    args = ["batch-update", "--filepat", filepath, "--session", session]
    # execution
    result = runner.invoke(traits, args)
    # assert
    assert result.exit_code == 2


def test_traits_batch_update_wrong_command(session):
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_update.csv")
    args = ["batch-updat", "--filepath", filepath, "--session", session]
    # execution
    result = runner.invoke(traits, args)
    # assert
    assert result.exit_code == 2
