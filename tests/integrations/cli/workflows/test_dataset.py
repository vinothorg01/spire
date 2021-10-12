from click.testing import CliRunner
import tempfile
import os
import csv
from csv import writer
from spire.framework.workflows.workflow import Workflow
from cli.workflows.datasets_cli import datasets


def test_update_dataset(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    vendor = "adobe"
    groups = "universe"
    args = ["update", "--id", str(wf.id), "--vendor", vendor, "--groups", groups]

    result = runner.invoke(datasets, args, input="y")
    print(result.output)
    assert "vendor: adobe" in result.output
    assert "logic: matches_any" in result.output
    assert "Dataset updated successfully" in result.output
    assert result.exit_code == 0


def test_update_dataset_multigroups(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    vendor = "ncs"
    source = "custom"
    groups = "test1,test2"
    args = [
        "update",
        "--id",
        str(wf.id),
        "--vendor",
        vendor,
        "--source",
        source,
        "--groups",
        groups,
    ]

    result = runner.invoke(datasets, args, input="y")
    assert "vendor: ncs" in result.output
    assert "group: test1" in result.output
    assert "group: test2" in result.output
    assert "logic: matches_any" in result.output
    assert "Dataset updated successfully" in result.output
    assert result.exit_code == 0


def test_update_dataset_wrong_data(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    vendor = "nc"
    source = "cust"
    groups = "test1,test2"
    args = [
        "update",
        "--id",
        str(wf.id),
        "--vendor",
        vendor,
        "--source",
        source,
        "--groups",
        groups,
    ]

    result = runner.invoke(datasets, args, input="y")
    assert "Dataset is not created due to insufficient data" in result.output
    assert result.exit_code == 0


def test_update_dataset_wrong_command(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    vendor = "nc"
    source = "cust"
    groups = "test1,test2"
    args = [
        "updat",
        "--id",
        wf.id,
        "--vendor",
        vendor,
        "--source",
        source,
        "--groups",
        groups,
    ]

    result = runner.invoke(datasets, args, input="y")
    assert result.exit_code == 2


def test_dataset_get(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    args = ["get", "--id", str(wf.id)]

    result = runner.invoke(datasets, args)
    assert result.exit_code == 0


def test_dataset_get_worng_option():
    # setup
    runner = CliRunner()
    args = ["get", "--ids", "326dd385-3b3c-4d3b-a368-accc5b0da01c"]

    result = runner.invoke(datasets, args)
    assert result.exit_code == 2


def test_dataset_get_worng_command():
    # setup
    runner = CliRunner()
    id = "326dd385-3b3c-4d3b-a368-accc5b0da01c"
    args = ["ge", "--ids", id]

    result = runner.invoke(datasets, args)
    assert result.exit_code == 2


def test_dataset_batch_update(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_create.csv")
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
        "schdeule",
    ]

    with open(filepath, "w", newline="") as csvfile:
        dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        dict_writer.writeheader()
    with open(filepath, "a", newline="") as write_obj:
        csv_writer = writer(write_obj)
        csv_writer.writerow(
            [
                str(wf.id),
                "cli-test",
                "cli-test",
                "True",
                "adobe",
                "",
                "test1",
                "",
                "",
                "",
            ]
        )
        csv_writer.writerow(
            [
                str(wf.id),
                "cli-test1",
                "cli-tes1t",
                "True",
                "ncs",
                "custom",
                "universe",
                "",
                "",
                "",
            ]
        )
    args = ["batch-update", "--filepath", filepath]

    # execution
    result = runner.invoke(datasets, args)
    # assert
    assert result.exit_code == 0


def test_dataset_batch_update_wrong_option(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_create.csv")
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
        "schdeule",
    ]
    with open(filepath, "w", newline="") as csvfile:
        dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        dict_writer.writeheader()
    with open(filepath, "a", newline="") as write_obj:
        csv_writer = writer(write_obj)
        csv_writer.writerow(
            [
                str(wf.id),
                "cli-test",
                "cli-test",
                "True",
                "adobe",
                "",
                "test1",
                "",
                "",
                "",
            ]
        )
        csv_writer.writerow(
            [
                str(wf.id),
                "cli-test1",
                "cli-tes1t",
                "True",
                "adobe",
                "",
                "test1",
                "",
                "",
                "",
            ]
        )
    args = ["batch-update", "--filepat", filepath]

    # execution
    result = runner.invoke(datasets, args)
    # assert
    assert result.exit_code == 2


def test_dataset_batch_update_wrong_command(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_create.csv")
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
        "schdeule",
    ]

    with open(filepath, "w", newline="") as csvfile:
        dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        dict_writer.writeheader()
    with open(filepath, "a", newline="") as write_obj:
        csv_writer = writer(write_obj)
        csv_writer.writerow(
            [
                str(wf.id),
                "cli-test",
                "cli-test",
                "True",
                "adobe",
                "",
                "test1",
                "",
                "",
                "",
            ]
        )
        csv_writer.writerow(
            [
                str(wf.id),
                "cli-test1",
                "cli-tes1t",
                "True",
                "adobe",
                "",
                "test1",
                "",
                "",
                "",
            ]
        )
    args = ["batch-updat", "--filepath", filepath]

    # execution
    result = runner.invoke(datasets, args)
    # assert
    assert result.exit_code == 2
