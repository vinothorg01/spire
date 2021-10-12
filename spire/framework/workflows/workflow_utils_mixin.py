import datetime
from collections import defaultdict
from requests.exceptions import RequestException

from spire.framework.workflows import Workflow

from spire.integrations.postgres import connector

from spire.utils import constants
from spire.utils.databricks import get_cluster_state, get_cluster_id
from spire.utils.general import assert_datetime_argument
from spire.utils.postgres_providers import query_postgres

from spire.config import config


class WorkflowUtilsMixin:
    def serialize_group(self, workflows):
        return [str(wf.id) for wf in workflows]

    def deserialize_group(self, workflow_ids, session=None):
        if not session:
            session = connector.make_session()
        return (
            session.query(Workflow).filter(Workflow.id.in_(workflow_ids)).all()
        )  # noqa

    def get_workflows_to_launch(self, stage, run_date):
        scheduled = self.get_scheduled_workflows(stage, run_date)
        return self.get_idle_workflows(stage, scheduled)

    def get_scheduled_workflows(self, stage, run_date):
        session = connector.make_session()
        ready_to_run = []
        enabled_workflows = Workflow.load_all_enabled(session)
        for workflow in enabled_workflows:
            ready = workflow.schedule.ready_to_run(stage, run_date)
            if ready:
                ready_to_run.append(workflow)
        return ready_to_run

    def get_scheduled_workflows_batch(self, stage, run_date):
        session = connector.make_session()
        scheduled_ids = self.get_scheduled_workflow_ids_batch(stage, run_date)
        return (
            session.query(Workflow).filter(Workflow.id.in_(scheduled_ids)).all()
        )  # noqa

    def get_scheduled_workflow_ids_batch(self, stage, run_date):
        conn_str = "host={} dbname={} user={} password={}".format(
            config.DB_HOSTNAME, config.DB_NAME, config.DB_USER, config.DB_PASSWORD
        )
        query = self.batch_query_scheduled_workflows(stage, run_date)
        # TODO: remove manual query
        return [obj["workflow_id"] for obj in query_postgres(conn_str, query)]

    def get_threshold_unit(self, unit, threshold):
        return {
            "seconds": datetime.timedelta(seconds=threshold),
            "days": datetime.timedelta(days=threshold),
        }[unit]

    def batch_query_scheduled_workflows(self, stage, date, unit="days", threshold=7):
        delta = self.get_threshold_unit(unit, threshold)
        threshold_date = date - delta
        return """
            SELECT workflow_id
            FROM (
                SELECT workflow_id, arg_date, rank() OVER (
                    PARTITION BY workflow_id ORDER BY arg_date DESC
                    ) as rank
                FROM history JOIN workflows
                ON history.workflow_id = workflows.id
                WHERE history.status = 'success'
                      AND history.stage = '{stage}'
                      AND workflows.enabled IS true
            ) subquery
            WHERE rank = 1 AND arg_date <= DATE('{threshold_date}')
            """.format(
            stage=stage, threshold_date=threshold_date
        )

    def get_workflows_per_cluster(self, stage, workflows):
        workflows_per_cluster = defaultdict(list)
        for wf in workflows:
            for c in range(len(wf.cluster_status)):
                if wf.cluster_status[c].stage == "{}_status".format(stage):
                    cluster_id = wf.cluster_status[c].cluster_id
                    workflows_per_cluster[cluster_id].append(wf)
        return workflows_per_cluster

    def filter_terminated_jobs(self, workflow_groups):
        results = []
        for k, v in workflow_groups.items():
            if not k:
                results += v
                continue
            try:
                cluster_state = get_cluster_state(k)
                if cluster_state == constants.RUN_TERMINATED:
                    results += v
            except RequestException:
                results += v
        return results

    def get_idle_workflows(self, stage, scheduled_workflows):
        workflow_groups = self.get_workflows_per_cluster(
            stage, scheduled_workflows
        )  # noqa
        return self.filter_terminated_jobs(workflow_groups)

    def update_cluster_statuses(self, stage, workflows, run_id):
        cluster_id = get_cluster_id(run_id)
        for wf in workflows:
            wf.update_cluster_status(stage, cluster_id, run_id)
        return

    def filter_failed_workflows(self, stage, date, workflow_ids):
        date = assert_datetime_argument(date)
        session = connector.make_session()
        workflows = self.deserialize_group(workflow_ids, session)
        status_reference = {
            wf: wf.last_successful_run_date(session, stage) for wf in workflows
        }
        return [k for k, v in status_reference.items() if v == date]
