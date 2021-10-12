import os
import csv
import tempfile
from click.testing import CliRunner
from cli.workflows.workflows_cli import workflows
from cli.workflows.schedules_cli import schedules

# NOTE(Max): even when not used, session is necessary for
# test database build and teardown


def test_workflows_create_default(session):
    # setup
    runner = CliRunner()
    name = "cli_test"
    description = "cli > test"
    args = ["create", "--name", name, "--description", description]

    # execution
    result = runner.invoke(workflows, args, input="y")
    # assert
    assert result.exit_code == 0

    # execution
    args = ["get", "--name", name]
    result = runner.invoke(workflows, args)
    # asset
    assert f"name: {name}" in result.output
    assert f"description: {description}" in result.output
    assert "trait_id: null" in result.output
    assert result.exit_code == 0


def test_workflows_create_enabled_false(session):
    # setup
    runner = CliRunner()
    name = "cli_test"
    description = "cli > test"
    args = ["create", "--name", name, "--description", description, "--no-enabled"]

    # execution
    result = runner.invoke(workflows, args, input="y")

    # assert
    assert result.exit_code == 0

    # execution
    args = ["get", "--name", name]
    result = runner.invoke(workflows, args)
    # asset
    assert f"name: {name}" in result.output
    assert f"description: {description}" in result.output
    assert "enabled: false" in result.output
    assert "trait_id: null" in result.output
    assert result.exit_code == 0


def test_workflows_create_schedule(session):
    # setup
    runner = CliRunner()
    name = "cli_test"
    description = "cli > test"
    args = ["create", "--name", name, "--description", description, "--adobe"]

    # execution
    result = runner.invoke(workflows, args, input="y")

    # assert
    assert result.exit_code == 0

    # execution
    args = ["get", "--name", name]
    result = runner.invoke(schedules, args)
    print(result.output)
    assert "logic: scheduled_stages" in result.output
    assert "assembly" in result.output
    assert "training" in result.output
    assert "scoring" in result.output
    assert result.exit_code == 0


def test_workflows_create_with_trait(session):
    # setup
    runner = CliRunner()
    name = "cli_test"
    description = "cli > test"
    trait_id = 9000
    args = [
        "create",
        "--name",
        name,
        "--description",
        description,
        "--trait-id",
        trait_id,
    ]

    # execution
    result = runner.invoke(workflows, args, input="y")

    # assert
    assert result.exit_code == 0

    # execution
    args = ["get", "--name", name]
    data = runner.invoke(workflows, args)
    assert data.exit_code == 0


def test_workflows_create_wrong_option():
    # setup
    runner = CliRunner()
    name = "cli_test"
    description = "cli > test"
    args = ["create", "--nam", name, "--descripton", description]

    # execution
    result = runner.invoke(workflows, args, input="y", catch_exceptions=False)
    # assert
    assert result.exit_code == 2


def test_workflows_wrong_create_command():
    # setup
    runner = CliRunner()
    name = "cli_test"
    description = "cli > test"
    args = ["creat", "--name", name, "--description", description]

    # execution
    result = runner.invoke(workflows, args, input="y", catch_exceptions=False)

    # assert
    assert result.exit_code == 2


def test_workflows_update(session):
    runner = CliRunner()
    name = "cli_test"
    description = "cli > test"
    args = ["create", "--name", name, "--description", description]

    # execution
    result = runner.invoke(workflows, args, input="y")

    name = "cli_test"
    enabled = "true"
    description = "cli_test_10"
    update_args = [
        "update",
        "--name",
        name,
        "--enabled",
        enabled,
        "--description",
        description,
    ]

    # execution
    result = runner.invoke(workflows, update_args, input="y")
    assert result.exit_code == 0

    get_args = ["get", "--name", name]
    # execution
    result = runner.invoke(workflows, get_args)
    assert result.exit_code == 0
    assert f"enabled: {enabled}" in result.output
    assert f"description: {description}" in result.output


def test_workflows_wrong_update_command():
    runner = CliRunner()
    name = "cli_test"
    enabled = "test"
    args = ["updat", "--name", name, "--enabled", enabled]

    # execution
    result = runner.invoke(workflows, args)
    assert result.exit_code == 2


def test_workflows_get():
    runner = CliRunner()
    name = "cli_test"
    args = ["get", "--name", name]

    # execution
    result = runner.invoke(workflows, args)
    assert result.exit_code == 0


def test_workflows_wrong_get_command():
    runner = CliRunner()
    name = "cli_test"
    args = ["ge", "--name", name]

    # execution
    result = runner.invoke(workflows, args)
    assert result.exit_code == 2


def test_workflows_export_batch_create_csv_filepath():
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, test_dir)
    args = ["export-batch-create-csv", "--filepath", filepath]

    # execution
    result = runner.invoke(workflows, args)
    assert result.exit_code == 0


def test_workflows_export_batch_create_wrong_option():
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, test_dir)
    args = ["export-batch-create-csv", "--filep", filepath]

    # execution
    result = runner.invoke(workflows, args)
    assert result.exit_code == 2


def test_workflows_export_batch_create_wrong_command():
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, test_dir)
    args = ["export-batch-crete-csv", "--filepath", filepath]

    # execution
    result = runner.invoke(workflows, args)
    assert result.exit_code == 2


def test_workflows_export_batch_update_csv_filepath():
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, test_dir)
    args = ["export-batch-update-csv", "--filepath", filepath]

    # execution
    result = runner.invoke(workflows, args)
    assert result.exit_code == 0


def test_workflows_export_batch_update_wrong_option():
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, test_dir)
    args = ["export-batch-update-csv", "--filep", filepath]

    # execution
    result = runner.invoke(workflows, args)
    assert result.exit_code == 2


def test_workflows_export_batch_update_wrong_command():
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, test_dir)
    args = ["export-batch-updat-csv", "--filepath", filepath]

    # execution
    result = runner.invoke(workflows, args)
    assert result.exit_code == 2


def test_workflows_batch_create(session):
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_create.csv")
    args = ["export-batch-create-csv", "--filepath", test_dir]
    # execution
    result = runner.invoke(workflows, args)
    with open(filepath, "a", newline="") as csvfile:
        create_writer = csv.writer(csvfile)
        create_writer.writerow(
            ["cli-test1", "cli > test1", False, "", "", "", ""]
        )
        create_writer.writerow(
            ["cli-test2", "cli > test2", False, "", "", "", ""]
        )
        create_writer.writerow(
            ["cli-test3", "cli > test3", False, "adobe", "mcid", [1], "15001"]
        )
    args = ["batch-create", "--filepath", filepath]

    # execution
    result = runner.invoke(workflows, args)
    # assert
    assert result.exit_code == 0


def test_workflows_batch_create_wrong_option():
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_create.csv")
    args = ["export-batch-create-csv", "--filepath", test_dir]
    # execution
    result = runner.invoke(workflows, args)
    with open(filepath, "w", newline="") as csvfile:
        create_writer = csv.writer(csvfile)
        create_writer.writerow(["cli-test1", "cli-test", "", "", "", "", True, False])
        create_writer.writerow(["cli-tes2", "cli-tes2", "", "", "", "", True, False])
        create_writer.writerow(
            [
                "cli-tes3",
                "cli-tes3",
                "universe",
                "15001",
                "jumpshot",
                "standard",
                True,
                False,
            ]
        )
    args = ["batch-create", "--filepa", filepath]

    # execution
    result = runner.invoke(workflows, args)
    # assert
    assert result.exit_code == 2


def test_workflows_batch_create_wrong_command():
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_create.csv")
    args = ["export-batch-create-csv", "--filepath", test_dir]
    # execution
    result = runner.invoke(workflows, args)
    with open(filepath, "w", newline="") as csvfile:
        create_writer = csv.writer(csvfile)
        create_writer.writerow(["cli-test1", "cli-test", "", "", "", "", True, False])
        create_writer.writerow(["cli-tes2", "cli-tes2", "", "", "", "", True, False])
        create_writer.writerow(
            [
                "cli-tes3",
                "cli-tes3",
                "universe",
                "15001",
                "jumpshot",
                "standard",
                True,
                False,
            ]
        )
    args = ["batch-crete", "--filepath", filepath]

    # execution
    result = runner.invoke(workflows, args)
    # assert
    assert result.exit_code == 2


def test_workflows_batch_update_wrong_option():
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_create.csv")
    args = ["batch-create", "--filepa", filepath]

    # execution
    result = runner.invoke(workflows, args)
    # assert
    assert result.exit_code == 2


def test_workflows_batch_update_wrong_command():
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    filepath = os.path.join(test_dir, "workflow_create.csv")
    args = ["batch-crete", "--filepath", filepath]
    # execution
    result = runner.invoke(workflows, args)
    # assert
    assert result.exit_code == 2
