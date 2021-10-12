from spire.framework.execution import JobRunner, JobStatus
from spire.framework.workflows.job_config import (
    OneNotebookJobConfig,
)


def run_mcid_segments_update(task_config, **context):

    job_config = OneNotebookJobConfig(
        target_module="spire.tasks.execute_mcid_segments_update",
        target_method="main",
        new_cluster=task_config["cluster_configuration"],
    )
    job = job_config.get_jobs()[0]
    job_status = JobRunner.get_runner_for_job(job).run()
    JobStatus.monitor_statuses([job_status])


def run_active_segments_update(task_config, **context):

    job_config = OneNotebookJobConfig(
        target_module="spire.tasks.execute_active_segments_update",
        target_method="main",
        new_cluster=task_config["cluster_configuration"],
        args={"execution_date_str": context["execution_date"].strftime("%Y-%m-%d")},
    )
    job = job_config.get_jobs()[0]
    job_status = JobRunner.get_runner_for_job(job).run()
    JobStatus.monitor_statuses([job_status])


def run_aam_segments_updates(task_config, **context):

    job_config = OneNotebookJobConfig(
        target_module="spire.tasks.execute_aam_segments_tables_updates",
        target_method="main",
        new_cluster=task_config["cluster_configuration"],
        args={"execution_date_str": context["execution_date"].strftime("%Y-%m-%d")},
    )
    job = job_config.get_jobs()[0]
    job_status = JobRunner.get_runner_for_job(job).run()
    JobStatus.monitor_statuses([job_status])
