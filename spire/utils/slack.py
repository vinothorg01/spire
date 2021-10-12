import slack
from slack.web.slack_response import SlackResponse
import datetime
import pandas as pd
from io import BytesIO, StringIO
from typing import List, Tuple, Optional, Union, Dict, Any
from asyncio import Future
from sqlalchemy.sql.expression import func
from sqlalchemy.sql.selectable import Alias
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.query import Query
from spire.utils import constants
from spire.framework.workflows import Workflow, History, Trait, WorkflowStages, Schedule
from spire.framework.execution import JobStatuses
from spire.config import config


# TODO(Max): The current approach to reporting different error states e.g. failures,
# mde, unlaunched, is an anti-pattern. This should be refactored in the future.


# TODO(Max): Make these constants? Derive in some other way?
not_failure_statuses = ["PENDING", "RUNNING", "SUCCESS"]
failure_statuses_names = [
    status.name for status in JobStatuses if status.name not in not_failure_statuses
]
failure_statuses = [JobStatuses[status] for status in failure_statuses_names]
stages = [WorkflowStages.ASSEMBLY, WorkflowStages.TRAINING, WorkflowStages.SCORING]


class SlackConnector:
    def __init__(self):
        try:
            self.slack_client = slack.WebClient(config.SLACK_TOKEN)
        except KeyError:
            raise Exception(
                "SlackConnector requires a slack token"
                " in environment variables in order to init"
            )
        self.name = constants.SLACKBOT_NAME
        self.on_call = constants.ON_CALL
        self.channel_id = None
        self.channel_name = None

    def get_channel_id(self):
        channels_call = self.slack_client.api_call("conversations.list")
        if channels_call["ok"]:
            for channel in channels_call["channels"]:
                if channel["name"] == self.channel_name:
                    return channel["id"]
        return None

    def send_message(
        self, message: List[Dict[Any, Any]]
    ) -> Union[Future, SlackResponse]:
        """
        Uses the Slack API to send the report of workflow responses to the
        datasci-spire-reports channel
        """
        return self.slack_client.chat_postMessage(
            channel=self.channel_name,
            blocks=message,
            text=message,
            username=self.name,
            icon_emoji=constants.SLACKBOT_AVATAR,
        )

    def _upload_csv(self, csv_content, file_name):
        self.slack_client.files_upload(
            channels=self.channel_name,
            content=csv_content,
            username=self.name,
            filename=file_name,
            filetype="csv",
        )

    def _upload_excel(self, data, file_name):
        self.slack_client.files_upload(
            channels=self.channel_name,
            file=data,
            username=self.name,
            filename=file_name,
            filetype="xlsx",
        )

    def _upload_df_as_excel(
        self, df: pd.DataFrame, file_name: str, index: Optional[int] = False
    ):
        """
        Converts a pandas dataframe to excel and uploads it as an attachment to the
        slack reporter channel. Used by `make_report` to produce the excel uploads of
        the reports of workflows that failed or failed to launch.
        """
        output = BytesIO()
        df.to_excel(output, index=index, engine="xlsxwriter")
        output.seek(0)
        self._upload_excel(output, file_name)

    def _upload_df_as_csv(self, df: pd.DataFrame, file_name: str) -> None:
        """
        Converts a pandas dataframe to csv and uploads it as an attachment to the
        slack reporter channel. Used by `make_report` to produce the csv uploads of
        the reports of workflows that failed or failed to launch.
        """
        output = StringIO()
        df.to_csv(output, index=False, header=True)
        self._upload_csv(output.getvalue(), file_name)

    def upload_df_to_slack(
        self, df: pd.DataFrame, file_name: str, upload_format: str
    ) -> None:
        """
        Wrapper function around uploading files to slack in different formats.
        Currently only excel and csv are supported.
        """
        if upload_format == "excel":
            self._upload_df_as_excel(df, file_name)
        else:
            self._upload_df_as_csv(df, file_name)


class SpireReporter(SlackConnector):
    def __init__(self, env):
        super().__init__()
        self.env = env
        self.channel_name = constants.SLACK_STD_OUT_CHANNEL
        self.channel_id = self.get_channel_id()

    def upload_info_files(self, info: Dict[str, any], missing_dfs: List[pd.DataFrame]):
        for stage, stage_info in info.items():
            for field, val in stage_info.items():
                if (field == "has_failed") and val is True:
                    self.upload_df_to_slack(
                        stage_info["failures_df"], f"{stage}_failures.csv", "csv"
                    )
            missing_df = missing_dfs[stage]
            if len(missing_df.index) > 0:
                self.upload_df_to_slack(missing_df, f"unlaunched_{stage}.csv", "csv")

    def _make_header_block(self, arg_date: datetime.datetime) -> Dict[str, Any]:
        env = config.DEPLOYMENT_ENV.upper()
        header_text = f"*SPIRE REPORT -- {arg_date} -- {env}*"
        return self._make_message_block(header_text)

    def _make_message_block(self, text: str) -> Dict[str, Any]:
        return {"type": "section", "text": {"type": "mrkdwn", "text": text}}

    def _make_info_message(
        self,
        arg_date: datetime.date,
        reports: Dict[str, str],
        counts: Dict[str, Dict[str, int]],
        failures_dfs: Dict[str, pd.DataFrame],
    ):
        header = self._make_header_block(arg_date)
        divider = {"type": "divider"}
        info = {}
        for stage in stages:
            stage_counts = counts[stage]
            stage_failures_df = failures_dfs[stage]
            # To account for various failures, make the failure query more generalized
            # failed_workflows = num_launched_workflows - num_successful_workflows
            num_failures = stage_counts.get("num_launched", 0) - stage_counts.get(
                "success", 0
            )
            has_failed = num_failures > 0
            stage_report = reports[stage]
            info[stage] = {
                "message": self._make_message_block(stage_report),
                "failures_df": stage_failures_df,
                "has_failed": has_failed,
            }
        full_message = [
            header,
            divider,
            info[str(WorkflowStages.ASSEMBLY)]["message"],
            divider,
            info[str(WorkflowStages.TRAINING)]["message"],
            divider,
            info[str(WorkflowStages.SCORING)]["message"],
            divider,
        ]
        if info[str(WorkflowStages.SCORING)]["has_failed"]:
            on_failure_text = "\n\n\t<@{0}> <@{1}> <@{2}>".format(*constants.ON_CALL)
            full_message.append(self._make_message_block(on_failure_text))
        return info, full_message

    def _format_counts_report(
        self, counts: Dict[str, Dict[str, int]]
    ) -> Dict[str, str]:
        reports = {}
        for stage in stages:
            stage_counts = counts[stage]
            report_text = (
                f"*{stage.capitalize()} Report*\n"
                f"\tTotal: {stage_counts.get('num_workflows')}\n"
                f"\tSucceeded: {stage_counts.get(str(JobStatuses.SUCCESS), 0)}\n"
                f"\tUnlaunched: {stage_counts.get('num_not_launched')}\n"
            )
            if stage != str(WorkflowStages.SCORING):
                report_text += (
                    f"\tFailures: {stage_counts.get(str(JobStatuses.FAILED), 0)}\n"
                )
            else:
                failures = sum(
                    [stage_counts.get(status, 0) for status in failure_statuses]
                )
                report_text += f"\tFailures: {failures}\n"
                categorized_failures = 0
                for status in failure_statuses_names:
                    if status == "FAILED":
                        continue
                    status_str = str(JobStatuses[status])
                    report_text += (
                        f"\t\t - {status_str+'s'}: "
                        f"{stage_counts.get(str(JobStatuses[status]), 0)}\n"
                    )
                    categorized_failures += stage_counts.get(
                        str(JobStatuses[status]), 0
                    )
                report_text += (
                    f"\t\t - Uncategorized Failures: "
                    f"{failures - categorized_failures}\n"
                )
            reports[stage] = report_text
        return reports

    def make_message(
        self,
        arg_date: datetime.date,
        counts: Dict[str, Dict[str, int]],
        failures_dfs: Dict[str, pd.DataFrame],
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
        reports = self._format_counts_report(counts)
        info, full_message = self._make_info_message(
            arg_date, reports, counts, failures_dfs
        )
        return info, full_message

    def _make_subquery(
        self, session: Session, stage: str, arg_date: datetime.datetime
    ) -> Alias:
        # TODO(Max): Refactor stage: str to stage: WorkflowStages
        """
        Query History objects for enabled workflows for a given stage and date,
        partitioned by workflow_id and ordered by execution date.
        """
        # Subset the data to the enabled workflows for the given arg date and stage
        data_subset = (
            session.query(History)
            .join(Workflow)
            .filter(Workflow.enabled)
            .filter(History.arg_date == arg_date)
            .filter(History.stage == stage)
            .subquery()
        )

        # For each enabled workflow id for the given stage,
        # order the records by execution date.
        # This is to ensure that the latest workflow status is retrieved
        # by subsequent queries.
        wf_ordered_by_exec_date = (
            session.query(
                History,
                func.row_number()
                .over(
                    partition_by=(History.workflow_id),
                    order_by=(History.execution_date.desc()),
                )
                .label("row_number"),
            )
            .select_entity_from(data_subset)
            .subquery()
        )

        return wf_ordered_by_exec_date

    def _get_histories(self, session: Session, q: Query) -> List[History]:
        """
        Queries for a list of History objects for the latest instance of each failure
        status from the subquery; All workflows with histories for a stage on an
        arg_date.
        """
        return (
            session.query(History)
            .select_entity_from(q)
            .filter((q.c.row_number == 1))
            .filter(History.status.in_(failure_statuses))
            .all()
        )

    def _get_scheduled_wfs(
        self, session: Session, arg_date: datetime.datetime, stage: WorkflowStages
    ) -> List[str]:
        """Get list of workflow ids scheduled to run on arg_date."""
        # TODO: Ideally, this should be refactored out of this class and
        # added to a query or API layer

        # Obtain the workflow schedules
        wf_schedule_expr = Schedule.definition[("args", stage, "args", "cron_interval")]
        wf_schedules = list(
            set(
                session.query(
                    Schedule.workflow_id, wf_schedule_expr.label("schedule")
                ).all()
            )
        )

        # Get the day the workflow is being run on
        # Cron schedules are ordered 0 - 7 wherein 0 and 7 are Sundays
        # Datetime schedules are ordered 1 - 7 wherein 1 is Monday
        day_of_run = arg_date.isoweekday() % 7

        # Get scheduled workflows
        scheduled_workflows = []
        for wf in wf_schedules:
            # Compare the day of the cron schedule to the day of the run
            # If they match, the wf is scheduled to be run on the day of the run
            if int(wf.schedule[-1]) == day_of_run:
                scheduled_workflows.append(wf.workflow_id)

        return scheduled_workflows

    def _get_unlaunched(
        self, session: Session, arg_date: datetime.datetime, stage: WorkflowStages
    ) -> pd.DataFrame:
        """
        Diff of all enabled workflows (all_workflows) and enabled workflows having
        history objects for arg_date and stage (present_workflows). These
        missing_workflows neither succeeded nor failed at stage; they were unlaunched.
        """
        present_workflows = list(
            set(
                session.query(Workflow.id)
                .filter(Workflow.enabled)
                .join(History)
                .filter(History.arg_date == arg_date)
                .filter(History.stage == str(stage))
                .all()
            )
        )
        # Scoring should all have the same arg_dates for the whole day, but this is not
        # the case for assembly/training which have variable schedules
        all_workflows = list(
            set(
                (
                    session.query(Workflow.id, Trait.trait_id, Workflow.description)
                    .join(Trait)
                    .filter(Workflow.enabled)
                    .filter(History.arg_date == arg_date)
                    .all()
                )
            )
        )

        # Subset the all_workflows list to only contain workflows that were scheduled
        # to run on arg_date for ASSEMBLY AND TRAINING. This logic does not apply to
        # SCORING as it runs every day for all workflows
        if stage != WorkflowStages.SCORING:
            # Get the workflows that were scheduled to run on arg_date
            scheduled_workflows = self._get_scheduled_wfs(session, arg_date, stage)
            # Get the workflows that were enabled for the arg_date AND scheduled to run
            # Extract wf id from all_wokflows
            enabled_wfs = [wf.id for wf in all_workflows]
            wfs_enabled_scheduled = list(set(enabled_wfs) & set(scheduled_workflows))
            updated_all_workflows = [
                workflow
                for workflow in all_workflows
                if workflow.id in wfs_enabled_scheduled
            ]
        else:
            updated_all_workflows = all_workflows

        missing_workflows = [
            workflow
            for workflow in updated_all_workflows
            if (workflow[0],) not in present_workflows
        ]
        return missing_workflows

    def _get_counts(
        self, session: Session, q: Query, missing_workflows: List[Workflow] = None
    ):
        num_workflows = len(Workflow.load_all_enabled(session))
        counts = dict(
            (
                session.query(History.status, func.count("*"))
                .select_entity_from(q)
                .filter((q.c.row_number == 1))
                .filter(History.status.in_(failure_statuses + [JobStatuses.SUCCESS]))
                .group_by(History.status)
                .all()
            )
        )
        counts["num_launched"] = sum([value for value in counts.values()])
        counts["num_workflows"] = num_workflows
        counts["num_not_launched"] = len(missing_workflows) or 0
        return counts

    def _make_report_df(
        self, histories: List[History], missing_workflows: List[Workflow]
    ) -> Optional[pd.DataFrame]:
        failures_report = None
        for history in histories:
            report_df = pd.DataFrame(history.to_dict())
            if not report_df.empty:
                report_df = report_df[["trait_id", "workflow_id", "ui_name", "error"]]
            if failures_report is None:
                failures_report = report_df
            else:
                failures_report.append(report_df)
        if len(missing_workflows) == 0:
            missing_report = pd.DataFrame(missing_workflows)
        else:
            missing_report = pd.DataFrame(
                missing_workflows, columns=["workflow_id", "trait_id", "description"]
            )
        return failures_report, missing_report

    def _get_stage_info(
        self, session: Session, stage: WorkflowStages, arg_date: datetime.datetime
    ) -> Tuple[Dict[str, int], pd.DataFrame]:
        q = self._make_subquery(session, stage, arg_date)
        latest_failures = self._get_histories(session, q)
        missing_workflows = self._get_unlaunched(session, arg_date, stage)
        counts = self._get_counts(session, q, missing_workflows)
        failures_df, missing_df = self._make_report_df(
            latest_failures, missing_workflows
        )
        return counts, failures_df, missing_df

    def get_info(
        self, session: Session, arg_date: datetime.datetime
    ) -> Tuple[
        Dict[str, Dict[str, int]], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]
    ]:
        """
        Runs a database query returning the following:

        counts: Dict of counts where stage is key and val is a dict where status is key
        and val is a list of History objects from subquery; the latest History for each
        workflow for stage for each JobStatuses type
        failures_dfs: Dict of pandas DataFrame of latest_failures where key is stage
        missing_dfs: Dict of pandas DataFrame of missing workflows where key is stage
        """
        counts = {}
        failures_dfs = {}
        missing_dfs = {}
        for stage in stages:
            count, failures_df, missing_df = self._get_stage_info(
                session, stage, arg_date
            )
            counts[str(stage)] = count
            failures_dfs[stage] = failures_df
            missing_dfs[stage] = missing_df
        return counts, failures_dfs, missing_dfs

    def make_report(self, session: Session, arg_date: datetime.datetime):
        counts, failures_dfs, missing_dfs = self.get_info(session, arg_date)
        info, message = self.make_message(arg_date, counts, failures_dfs)
        self.send_message(message)
        self.upload_info_files(info, missing_dfs)
