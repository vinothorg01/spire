import datetime
from typing import Dict, Any, List
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import func

from spire.framework.workflows import Workflow, History
from spire.api import utils
from spire.integrations import connector


@connector.session_transaction
def get_report(
    wf_id: str = None, wf_name: str = None, session: Session = None
) -> Dict[str, Any]:
    """
    From a wf_id or wf_name, returns a dict report of the most recent successful run
    for the workflow

    Requires either wf_id or wf_name

    Args:
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        session: SQLAlchemy session
    Returns:
        History dict
    """
    wf = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    q = (
        session.query(  # type: ignore
            History,
            func.row_number()
            .over(
                partition_by=(History.workflow_id, History.stage),
                order_by=History.execution_date.desc(),
            )
            .label("row_number"),
        )
        .filter(History.workflow_id == wf.id)
        .filter(History.status == "success")
        .subquery()
    )
    result = (
        session.query(History).select_entity_from(q).filter((q.c.row_number == 1)).all()
    )
    reports = {}
    for report in result:
        if "assembly" in reports and "training" in reports and "scoring" in reports:
            return reports
        if report.stage not in reports:
            reports[report.stage] = report.to_dict()
    return reports


@connector.session_transaction
def get_reports(
    stage: str,
    arg_date: datetime.datetime = None,
    as_of: datetime.datetime = None,
    session: Session = None,
) -> List[Dict[str, Any]]:
    """
    Given a stage (i.e. assembly, training, scoring), and either a specific date
    (arg_date) or starting from a date (as_of), returns a list of dict reports of the
    most recent successful runs for all enabled workflows
    for the workflow

    Requires either arg_date, or as_of, but cannot specify both

    Args:
        stage: str (assembly, training, scoring)
        arg_date: datetime.datetime of date for report
        as_of: datetime.datetime date threshold (this date and all more recent dates)
        session: SQLAlchemy session
    Returns:
        List of History dicts
    """
    if arg_date and as_of:
        raise Exception("Cannot specify both arg_date " "and as_of conditions")
    if arg_date:
        date_condition = func.date(History.arg_date) == arg_date
    elif as_of:
        date_condition = History.execution_date <= as_of
    else:
        raise Exception("Must specify either arg_date or as_of")
    q = (
        session.query(
            History,
            func.row_number()
            .over(
                partition_by=(History.workflow_id, History.stage),
                order_by=(
                    History.status != "success",
                    History.execution_date.desc(),
                ),
            )
            .label("row_number"),
        )
        .filter(History.stage == stage)
        .filter(date_condition)
        .subquery()
    )
    result = (
        session.query(History, Workflow.enabled)
        .select_entity_from(q)
        .filter((q.c.row_number == 1))
        .join(Workflow)
        .all()
    )
    output = []
    for r, e in result:
        response = r.to_dict()
        response["enabled"] = e
        output.append(response)
    return output


@connector.session_transaction
def last_successful_run_date(
    stage: str, wf_id: str = None, wf_name: str = None, session: Session = None
) -> Dict[str, datetime.datetime]:
    """
    From a wf_id or wf_name, returns a dict of the most recent successful run date for
    the workflow at a given stage

    Requires either wf_id or wf_name

    Args:
        stage: str (assembly, training, scoring)
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        session: SQLAlchemy session
    Returns:
        History date dict
    """
    wf = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    result = wf.last_successful_run_date(session, stage)

    if result is not None:
        return {"last_successful_run_date": result.arg_date}  # type: ignore
    else:
        return None


@connector.session_transaction
def get_recent_failures(
    stage: str,
    wf_id: str = None,
    wf_name: str = None,
    as_of: datetime.datetime = datetime.datetime.today(),
    seconds: int = 0,
    session: Session = None,
) -> List[History]:
    """
    Given a stage (i.e. assembly, training, scoring), and either a wf_id or wf_name,
    returns a list of dict reports of failed runs for the workflow. If an as_of date is
    not provided, defaults to today. To subset the range further, seconds can be
    provided to subtract minutes, hours, or more from the as_of range

    Requires either wf_id or wf_name

    Args:
        stage: str (assembly, training, scoring)
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        as_of: datetime.datetime date threshold (this date and all more recent dates)
        seconds: int of number of seconds to subtract from the as_of range
        session: SQLAlchemy session
    Returns:
        List of History dicts
    """
    wf = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    return wf.get_recent_failures(session, stage, seconds, as_of)


@connector.session_transaction
def get_failures(
    stage: str,
    date: datetime.date = datetime.date.today() - datetime.timedelta(days=1),
    session: Session = None,
) -> List[History]:
    """
    Given a stage (i.e. assembly, training, scoring), returns a list of dict reports of
    failed runs for all workflows on the given date

    Args:
        stage: str (assembly, training, scoring)
        date: datetime.date (defaults to yesterday)
        seconds: int of number of seconds to subtract from the as_of range
        session: SQLAlchemy session
    Returns:
        List of History dicts
    """
    q = (
        session.query(
            History,
            func.row_number()
            .over(
                partition_by=(History.workflow_id, History.stage),
                order_by=(
                    History.status != "success",
                    History.execution_date.desc(),
                ),
            )
            .label("row_number"),
        )
        .filter(History.arg_date == date)
        .filter(History.stage == stage)
        .subquery()
    )
    return (
        session.query(History)
        .select_entity_from(q)
        .filter((q.c.row_number == 1))
        .filter(History.status != "success")
        .all()
    )


@connector.session_transaction
def get_stage_response(
    stage: str,
    arg_date: datetime.datetime = None,
    as_of: datetime.datetime = None,
    ignore_failures: bool = False,
    wf_id: str = None,
    wf_name: str = None,
    session: Session = None,
) -> Dict[str, Any]:
    """
    From a wf_id or wf_name, returns a dict report of the most recent run
    for the workflow at a given stage, either from a date (arg_date) or as of a given
    date (as_of)

    Requires either wf_id or wf_name

    Args:
        stage: str (assembly, training, scoring)
        arg_date: datetime.datetime of date for report
        as_of: datetime.datetime date threshold (this date and all more recent dates)
        ignore_failures: bool whether or not to drop failed runs from the query
        wf_id: workflow id as str (converted to uuid)
        wf_name: workflow name as str
        session: SQLAlchemy session
    Returns:
        History dict
    """
    wf = utils.get_by_id_or_name(wf_id=wf_id, wf_name=wf_name, session=session)
    if ignore_failures:
        status_condition = History.status == "success"
    else:
        status_condition = True
    if arg_date:
        date_condition = func.date(History.arg_date) == arg_date
    else:
        date_condition = True
    if as_of:
        as_of_condition = History.execution_date <= as_of
    else:
        as_of_condition = True

    q = (
        session.query(
            History,
            func.row_number()
            .over(order_by=History.execution_date.desc())
            .label("row_number"),
        )
        .filter(History.workflow_id == wf.id)
        .filter(History.stage == stage)
        .filter(History.execution_date is not None)
        .filter(status_condition)
        .filter(date_condition)
        .filter(as_of_condition)
        .subquery()
    )  # noqa E711
    history = (
        session.query(History).select_entity_from(q).filter(q.c.row_number == 1).one()
    )
    return history.to_dict()
