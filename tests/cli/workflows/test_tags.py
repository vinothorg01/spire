from click.testing import CliRunner
from unittest.mock import MagicMock
from cli.workflows.workflows_cli import workflows
from cli.workflows.tags_cli import tags
import tempfile
import os
import csv
from csv import writer


def test_add_get_tags():
    # setup
    runner = CliRunner()
    session = MagicMock()
    name = "cli_test"
    description = "cli > test"
    args = [
        "create",
        "--name",
        name,
        "--description",
        description,
        "default-schedule",
        "--session",
        session,
    ]

    # execution
    result = runner.invoke(workflows, args, input="y")
    wf = session.workflow
    print(wf.id)
    args = ["add", "--id", wf.id, "--tag-label", "fashion", "--session", session]

    result = runner.invoke(tags, args, input="y")
    print(result.output)
    assert "Tag fashion added successfully" in result.output
    assert result.exit_code == 0

    args = ["get", "--id", wf.id, "--session", session]

    result = runner.invoke(tags, args)
    assert result.exit_code == 0

    # cleanup
    session.close()


def test_tag_get_wrong_options():
    # setup
    runner = CliRunner()
    session = MagicMock()
    args = ["get", "--i", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1", "--session", session]

    result = runner.invoke(tags, args)
    assert result.exit_code == 2

    # cleanup
    session.close()


def test_tag_get_wrong_command():
    # setup
    runner = CliRunner()
    session = MagicMock()
    args = ["gt", "--id", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1", "--session", session]

    result = runner.invoke(tags, args)
    assert result.exit_code == 2

    # cleanup
    session.close()


def test_tag_add_wrong_options():
    # setup
    runner = CliRunner()
    session = MagicMock()
    args = ["add", "--i", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1", "--session", session]

    result = runner.invoke(tags, args)
    assert result.exit_code == 2

    # cleanup
    session.close()


def test_tag_add_wrong_command():
    # setup
    runner = CliRunner()
    session = MagicMock()
    args = ["ad", "--id", "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1", "--session", session]

    result = runner.invoke(tags, args)
    assert result.exit_code == 2

    # cleanup
    session.close()


def test_add_remove_tags():
    # setup
    runner = CliRunner()
    session = MagicMock()
    name = "cli_test"
    description = "cli > test"
    args = [
        "create",
        "--name",
        name,
        "--description",
        description,
        "default-schedule",
        "--session",
        session,
    ]

    # execution
    result = runner.invoke(workflows, args, input="y")
    wf = session.workflow
    args = ["add", "--id", wf.id, "--tag-label", "fashion", "--session", session]

    result = runner.invoke(tags, args, input="y")
    assert "added successfully" in result.output
    assert result.exit_code == 0

    args = ["remove", "--id", wf.id, "--tag-label", "fashion", "--session", session]

    result = runner.invoke(tags, args, input="y")
    assert result.exit_code == 0

    # cleanup
    session.close()


def test_tag_remove_wrong_options():
    # setup
    runner = CliRunner()
    session = MagicMock()
    args = [
        "remove",
        "--i",
        "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1",
        "--session",
        session,
    ]

    result = runner.invoke(tags, args)
    assert result.exit_code == 2

    # cleanup
    session.close()


def test_tag_remove_wrong_command():
    # setup
    runner = CliRunner()
    session = MagicMock()
    args = [
        "remov",
        "--id",
        "8c0fd8e9-a0c5-4201-b86f-abcda11f89b1",
        "--session",
        session,
    ]

    result = runner.invoke(tags, args)
    assert result.exit_code == 2

    # cleanup
    session.close()


def test_tag_batch_add():
    # setup
    runner = CliRunner()
    session = MagicMock()
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
        "tags",
    ]
    with open(filepath, "w", newline="") as csvfile:
        dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        dict_writer.writeheader()
    with open(filepath, "a", newline="") as write_obj:
        csv_writer = writer(write_obj)
        csv_writer.writerow(
            [
                "326dd385-3b3c-4d3b-a368-accc5b0da01c",
                "cli-test",
                "cli-test",
                "True",
                "",
                "",
                "",
                "",
                "",
                "",
                "fashion,food",
            ]
        )
        csv_writer.writerow(
            [
                "326dd385-3b3c-4d3b-a368-accc5b0da01d",
                "cli-test",
                "cli-test",
                "True",
                "",
                "",
                "",
                "",
                "",
                "",
                "entertainment",
            ]
        )
    args = ["batch-update", "--filepath", filepath, "--session", session]

    # execution
    result = runner.invoke(tags, args)
    print(result.output)
    # assert
    assert "tag fashion added to workflow" in result.output
    assert "tag food added to workflow" in result.output
    assert "tag entertainment added to workflow" in result.output
    assert result.exit_code == 0

    # cleanup
    session.close()


def test_tag_batch_add_wrong_option():
    # setup
    runner = CliRunner()
    session = MagicMock()
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
        "tags",
    ]
    with open(filepath, "w", newline="") as csvfile:
        dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        dict_writer.writeheader()
    with open(filepath, "a", newline="") as write_obj:
        csv_writer = writer(write_obj)
        csv_writer.writerow(
            [
                "326dd385-3b3c-4d3b-a368-accc5b0da01c",
                "cli-test",
                "cli-test",
                "True",
                "",
                "",
                "",
                "",
                "",
                "",
                "fashion,food",
            ]
        )
    args = ["batch-update", "--filepat", filepath, "--session", session]

    # execution
    result = runner.invoke(tags, args)
    print(result.output)
    # assert
    assert result.exit_code == 2

    # cleanup
    session.close()


def test_tag_batch_add_wrong_command():
    # setup
    runner = CliRunner()
    session = MagicMock()
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
        "tags",
    ]
    with open(filepath, "w", newline="") as csvfile:
        dict_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        dict_writer.writeheader()
    with open(filepath, "a", newline="") as write_obj:
        csv_writer = writer(write_obj)
        csv_writer.writerow(
            [
                "326dd385-3b3c-4d3b-a368-accc5b0da01c",
                "cli-test",
                "cli-test",
                "True",
                "",
                "",
                "",
                "",
                "",
                "",
                "fashion,food",
            ]
        )
    args = ["batch-update-remov", "--filepath", filepath, "--session", session]

    # execution
    result = runner.invoke(tags, args)
    print(result.output)
    # assert
    assert result.exit_code == 2

    # cleanup
    session.close()
