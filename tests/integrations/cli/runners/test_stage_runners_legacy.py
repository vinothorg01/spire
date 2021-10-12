from click.testing import CliRunner
from unittest.mock import MagicMock
from spire.framework.workflows.workflow import Workflow
from cli.runners.stage_runners_cli import stage_runners
from spire.config import config


def test_legacy_assembly_dryrun(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    config.VAULT_TOKEN = "qwrwerweq"
    rundate = "2020-01-12"
    task = "assembly"
    args = [
        "run-legacy",
        "--id",
        str(wf.id),
        "--run-date",
        rundate,
        "--session",
        session,
        "--task",
        task,
        "--dryrun",
    ]
    result = runner.invoke(stage_runners, args)
    assert result.exit_code == 0


def test_legacy_docker_image(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    config.VAULT_TOKEN = "qwrwerweq"
    rundate = "2020-01-12"
    task = "training"
    args = [
        "run-legacy",
        "--id",
        str(wf.id),
        "--run-date",
        rundate,
        "--session",
        session,
        "--task",
        task,
        "--spire_image",
        "spire-dev",
        "--spire_version",
        "3.3.3",
        "--dryrun",
    ]
    result = runner.invoke(stage_runners, args)
    print(result.output)
    assert "spire/train/runner" in result.output
    assert '"date": "2020-01-12"' in result.output
    assert str(wf.id) in result.output
    assert result.exit_code == 0


def test_legacy_training_dryrun(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    config.VAULT_TOKEN = "qwrwerweq"
    rundate = "2020-01-12"
    task = "training"
    args = [
        "run-legacy",
        "--id",
        str(wf.id),
        "--run-date",
        rundate,
        "--session",
        session,
        "--task",
        task,
        "--dryrun",
    ]

    result = runner.invoke(stage_runners, args)
    print(result.output)
    assert "spire/train/runner" in result.output
    assert '"date": "2020-01-12"' in result.output
    assert str(wf.id) in result.output
    assert result.exit_code == 0


def test_legacy_scoring_dryrun(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    config.VAULT_TOKEN = "qwrwerweq"
    rundate = "2020-01-12"
    task = "scoring"
    args = [
        "run-legacy",
        "--id",
        str(wf.id),
        "--run-date",
        rundate,
        "--session",
        session,
        "--task",
        task,
        "--dryrun",
    ]

    result = runner.invoke(stage_runners, args)
    assert "spire/score/runner" in result.output
    assert '"date": "2020-01-12"' in result.output
    assert str(wf.id) in result.output
    assert result.exit_code == 0


def test_legacy_backfill_dryrun(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    config.VAULT_TOKEN = "qwrwerweq"
    rundate = "2020-12-02"
    task = "scoring"
    days_to_backfill = 2
    args = [
        "run-legacy",
        "--id",
        str(wf.id),
        "--run-date",
        rundate,
        "--days-to-backfill",
        days_to_backfill,
        "--session",
        session,
        "--task",
        task,
        "--dryrun",
    ]

    result = runner.invoke(stage_runners, args)
    assert "spire/score/runner" in result.output
    assert '"date": "2020-12-02"' in result.output
    assert '"date": "2020-12-01"' in result.output
    assert str(wf.id) in result.output
    assert result.exit_code == 0


def test_legacy_specifc_dates_dryrun(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)

    config.VAULT_TOKEN = "qwrwerweq"
    specific_dates = "2020-12-02,2020-12-01"
    task = "scoring"
    args = [
        "run-legacy",
        "--id",
        str(wf.id),
        "--specific-dates",
        specific_dates,
        "--session",
        session,
        "--task",
        task,
        "--dryrun",
    ]

    result = runner.invoke(stage_runners, args)
    assert "spire/score/runner" in result.output
    assert '"date": "2020-12-02"' in result.output
    assert '"date": "2020-12-01"' in result.output
    assert str(wf.id) in result.output
    assert result.exit_code == 0


def test_legacy_wrong_option():
    # setup
    runner = CliRunner()
    session = MagicMock()

    wf_id = "9b0f19d2-05a9-4504-af6f-15c41b80f859"
    specific_dates = "2020-12-02,2020-12-01"
    task = "scoring"
    args = [
        "run-legacy",
        "--id",
        wf_id,
        "--specific-date",
        specific_dates,
        "--session",
        session,
        "--task",
        task,
        "--dryrun",
    ]

    result = runner.invoke(stage_runners, args)
    assert result.exit_code == 1


def test_legacy_wrong_command():
    # setup
    runner = CliRunner()
    session = MagicMock()

    wf_id = "9b0f19d2-05a9-4504-af6f-15c41b80f859"
    specific_dates = "2020-12-02,2020-12-01"
    task = "scoring"
    args = [
        "ru-legacy",
        "--id",
        wf_id,
        "--specific-date",
        specific_dates,
        "--session",
        session,
        "--task",
        task,
        "--dryrun",
    ]

    result = runner.invoke(stage_runners, args)
    assert result.exit_code == 2
