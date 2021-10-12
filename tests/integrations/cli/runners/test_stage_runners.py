import pytest
from click.testing import CliRunner
from spire.framework.workflows.workflow import Workflow
from cli.runners.stage_runners_cli import stage_runners
from spire.config import config


TASKS = ["assembly", "training", "scoring"]


# TODO(Max): Many of the CLI tests were designed before much of our current tooling.
# These tests are currently unecessarily brittle given what we can do now


def test_run(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    config.VAULT_TOKEN = "qwrwerweq"
    rundate = "2020-01-12"
    for task in TASKS:
        args = [
            "run",
            "--wf-ids",
            str(wf.id),
            "--run-date",
            rundate,
            "--task",
            task,
            "--dryrun",
        ]
        result = runner.invoke(stage_runners, args)
        assert result.exit_code == 0


def test_run_backfill(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    config.VAULT_TOKEN = "qwrwerweq"
    rundate = "2020-12-02"
    days_to_backfill = 2
    args = [
        "run",
        "--wf-ids",
        str(wf.id),
        "--run-date",
        rundate,
        "--days-to-backfill",
        days_to_backfill,
        "--task",
        "scoring",
        "--dryrun",
    ]

    result = runner.invoke(stage_runners, args)
    results = result.output.split("\n")
    assert "2020-12-02" in results[1]
    assert "2020-12-01" in results[2]
    assert result.exit_code == 0


def test_run_specific_dates(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    config.VAULT_TOKEN = "qwrwerweq"
    specific_dates = "2020-12-02,2020-12-01"
    args = [
        "run",
        "--wf-ids",
        str(wf.id),
        "--specific-dates",
        specific_dates,
        "--task",
        "scoring",
        "--dryrun",
    ]

    result = runner.invoke(stage_runners, args)
    results = result.output.split("\n")
    assert "2020-12-02" in results[1]
    assert "2020-12-01" in results[2]
    assert result.exit_code == 0


@pytest.mark.skip(
    "Given how run_stage and run_stage_by_wf_ids have been consolidated, this approach"
    "for handling a mistaken wf_ids argument no longer applies"
)
def test_run_wrong_option():
    # setup
    runner = CliRunner()
    wf_id = "9b0f19d2-05a9-4504-af6f-15c41b80f859"
    specific_dates = "2020-12-02,2020-12-01"
    args = [
        "run",
        "--ids",  # NOT wf-ids
        wf_id,
        "--specific-dates",
        specific_dates,
        "--task",
        "scoring",
        "--dryrun",
    ]
    result = runner.invoke(stage_runners, args)
    assert result.exit_code == 1


def test_run_wrong_command():
    # setup
    runner = CliRunner()
    wf_id = "9b0f19d2-05a9-4504-af6f-15c41b80f859"
    specific_dates = "2020-12-02,2020-12-01"
    args = [
        "ru",  # NOT run
        "--wf-ids",
        wf_id,
        "--specific-date",
        specific_dates,
        "--task",
        "scoring",
        "--dryrun",
    ]

    result = runner.invoke(stage_runners, args)
    assert result.exit_code == 2
