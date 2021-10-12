from click.testing import CliRunner
from spire.framework.workflows.workflow import Workflow
from cli.workflows.queries_cli import queries
import tempfile


def test_query_count_enabled(session, wf_def):
    # setup
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    runner = CliRunner()
    args = ["count-enabled", "--session", session]

    result = runner.invoke(queries, args)
    assert result.exit_code == 0


def test_query_get_by_name(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    name = "spire_foo_test"
    args = ["get-by-name", "--name", name, "--session", session]

    result = runner.invoke(queries, args)
    assert result.exit_code == 0


def test_query_get_by_name_like(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    name = "spire_foo_test"
    args = ["get-by-name-like", "--name-like", name, "--session", session]

    result = runner.invoke(queries, args)
    assert result.exit_code == 0


def test_query_name_export_filepath(session, wf_def):
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    name = "spire_foo_test"
    args = [
        "get-by-name-like",
        "--name-like",
        name,
        "--export",
        "--filepath",
        test_dir,
        "--session",
        session,
    ]

    result = runner.invoke(queries, args)
    assert result.exit_code == 0


def test_query_get_by_description(session, wf_def):
    # setup
    runner = CliRunner()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    description = "test > test"
    args = ["get-by-description", "--description", description, "--session", session]

    result = runner.invoke(queries, args)
    assert result.exit_code == 0


def test_query_description_export_filepath(session, wf_def):
    # setup
    runner = CliRunner()
    test_dir = tempfile.mkdtemp()
    wf = Workflow.create_default_workflow(**wf_def, session=session)
    session.add(wf)
    # assert
    description = "test > test"
    args = [
        "get-by-description-like",
        "--description-like",
        description,
        "--export",
        "--filepath",
        test_dir,
        "--session",
        session,
    ]

    result = runner.invoke(queries, args)
    assert result.exit_code == 0
