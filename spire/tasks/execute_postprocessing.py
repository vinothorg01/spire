import click
from datetime import datetime, date

from typing import List
from pyspark.sql.session import SparkSession
from sqlalchemy.orm.session import Session
from spire.integrations.postgres import connector
from spire.framework.postprocess.postprocessing import postprocess_all
from spire.framework.workflows import Workflow
from spire.utils.spark_helpers import spark_session


@spark_session
@connector.session_transaction
def main(
    run_date: str,
    workflow_ids: List[str],
    spark: SparkSession = None,
    session: Session = None,
    **kwargs,
):
    run_date = datetime.strptime(run_date, "%Y-%m-%d").date()
    workflows = Workflow.get_by_ids(session, workflow_ids)
    return postprocess_all(workflows, run_date, session, spark)


# click.command doesn't play nicely with sys.modules, so we have to separate
# them.
@click.command()
@click.option("--workflow_ids")
@click.option("--run_date")
def cli_main(workflow_ids=None, run_date: str = None):
    if isinstance(run_date, str):
        run_date = datetime.strptime(run_date, "%Y-%m-%d").date()
    else:
        run_date = date.today()

    main(workflow_ids, run_date)


if __name__ == "__main__":
    cli_main()
