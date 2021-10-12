import json

from spire.framework.runners import JobRunner
from spire.utils import Logger, group_by_vendor
from spire.framework.workflows import WorkflowUtilsMixin
from spire.utils import chunk_into_groups

from spire.framework import constants


class SpireStageRunner(JobRunner, WorkflowUtilsMixin, Logger):
    def format_job_data(self, date, workflows, **kwargs):
        # fields always included in request
        base = {
            "date": date.strftime("%Y-%m-%d"),
            "workflow_ids": json.dumps(self.serialize_group(workflows)),
            "threadpool_group_size": constants.THREADPOOL_GROUP_SIZE,
        }
        # add any passed kwargs:
        base_copy = base.copy()
        base_copy.update(**kwargs)
        return base_copy

    def prep_for_launch(self, workflows, max_per_cluster=50):
        ready = self.get_idle_workflows(self.task_name, workflows)
        if not ready or len(ready) < constants.WF_THRESHOLD_MIN:
            return None
        return ready[:max_per_cluster]

    def launch_groups(self, date, workflows, max_per_cluster, dry_run=False):
        run_ids = []
        grouped = chunk_into_groups(workflows, max_per_cluster)
        for group in grouped:
            ready = self.prep_for_launch(group, max_per_cluster)
            if not ready:
                self.logger.info(
                    "No ready workflows for stage {}".format(self.task_name)
                )
            job_data = self.format_job_data(date, group)
            run_id = self.launch_task(job_data, dry_run)
            if dry_run:
                return run_id
            run_ids.append(run_id)
            self.logger.info(run_id)
            self.update_cluster_statuses(self.task_name, group, run_id)
        return run_ids

    def run(self, date, workflows=[], max_per_cluster=50, dry_run=False):
        if not workflows:
            workflows = self.get_workflows_to_launch(self.task_name, date)
        self.inject_vault_token()
        run_ids = self.launch_groups(date, workflows, max_per_cluster, dry_run)  # noqa
        return run_ids, self.serialize_group(workflows)


class AssemblyRunner(SpireStageRunner):

    params = {
        "task_name": constants.ASSEMBLY,
        "run_name": constants.ASSEMBLY_RUN,
        "cluster_type": constants.ASSEMBLY_CLUSTER_TYPE,
        "libraries": constants.DEPENDENCIES,
        "task_type": constants.ASSEMBLY_TASK_TYPE,
        "runner_script": constants.ASSEMBLY_RUNNER_SCRIPT,
        "wheels": constants.WHEEL_DEPS,
        "timeout_seconds": constants.ASSEMBLY_TIMEOUT_SECONDS,
        "docker_image": constants.DOCKER_IMAGE_CONFIG,
    }

    def __init__(self, **kwargs):
        default_params = AssemblyRunner.params.copy()
        default_params.update(kwargs)

        super(AssemblyRunner, self).__init__(**default_params)

    def format_job_data(self, date, workflows, max_per_cluster=20, **kwargs):
        limited_wfs = workflows[:max_per_cluster]
        return {
            "date": date.strftime("%Y-%m-%d"),
            "workflow_ids": json.dumps(self.serialize_group(limited_wfs)),
            "threadpool_group_size": constants.THREADPOOL_GROUP_SIZE,
            "vendor": kwargs["vendor"],
        }

    def launch_groups(
        self, date, wfs_by_vendor, max_per_cluster=20, dry_run=False
    ):  # noqa
        run_ids = []
        self.inject_vault_token()
        for vendor, wfs in wfs_by_vendor.items():
            self.logger.info("preparing wfs for vendor {}".format(vendor))
            prepped = self.prep_for_launch(wfs, max_per_cluster)
            if not prepped:
                self.logger.info(
                    constants.INSUFFICIENT_WORKFLOW_COUNT.format(
                        0, constants.WF_THRESHOLD_MIN
                    )
                )
                self.logger.info(constants.SKIPPING_VENDOR.format(vendor))
                continue
            job_data = self.format_job_data(
                date, prepped, max_per_cluster, **{"vendor": vendor}
            )  # noqa
            run_id = self.launch_task(job_data, dry_run)
            self.logger.info("launched run id: {} for vendor {}".format(run_id, vendor))
            if dry_run:
                return run_id
            run_ids.append(run_id)
            self.update_cluster_statuses(self.task_name, prepped, run_id)
            self.logger.info(
                "updated cluster statuses for wfs for vendor {}".format(vendor)
            )
        return run_ids

    def run(self, date, workflows=[], max_per_cluster=20, dry_run=False):
        """
        NB: lets think of this function as temporary
            until we can work out a different approach to assembly
        """
        run_ids = []
        # load all scheduled workflows if none are passed
        if not workflows:
            scheduled = self.get_scheduled_workflows(self.task_name, date)
            grouped_by_vendor = group_by_vendor(scheduled)
        else:
            grouped_by_vendor = group_by_vendor(workflows)
        # if no workflows scheduled, return two empty lists,
        # one for run ids, one for wfs
        if not grouped_by_vendor:
            return [], []

        # launch tasks by vendor
        run_ids = self.launch_groups(
            date, grouped_by_vendor, max_per_cluster, dry_run
        )  # noqa
        serialized_wfs = self.serialize_group(
            [wfs for group in grouped_by_vendor.values() for wfs in group]
        )  # noqa
        return run_ids, serialized_wfs


class TrainingRunner(SpireStageRunner):
    params = {
        "task_name": constants.TRAINING,
        "run_name": constants.TRAINING_RUN,
        "cluster_type": constants.TRAINING_CLUSTER_TYPE,
        "libraries": constants.DEPENDENCIES,
        "task_type": constants.TRAINING_TASK_TYPE,
        "runner_script": constants.TRAINING_RUNNER_SCRIPT,
        "wheels": constants.WHEEL_DEPS,
        "timeout_seconds": constants.TRAINING_TIMEOUT_SECONDS,
        "docker_image": constants.DOCKER_IMAGE_CONFIG,
    }

    def __init__(self, **kwargs):
        default_params = TrainingRunner.params.copy()
        default_params.update(kwargs)

        super(TrainingRunner, self).__init__(**default_params)


class ScoringRunner(SpireStageRunner):
    params = {
        "task_name": constants.SCORING,
        "run_name": constants.SCORING_RUN,
        "cluster_type": constants.SCORING_CLUSTER_TYPE,
        "libraries": constants.DEPENDENCIES,
        "task_type": constants.SCORING_TASK_TYPE,
        "runner_script": constants.SCORING_RUNNER_SCRIPT,
        "wheels": constants.WHEEL_DEPS,
        "timeout_seconds": constants.SCORING_TIMEOUT_SECONDS,
        "docker_image": constants.DOCKER_IMAGE_CONFIG,
    }

    def __init__(self, **kwargs):
        default_params = ScoringRunner.params.copy()
        default_params.update(kwargs)

        super(ScoringRunner, self).__init__(**default_params)


class PostProcessingRunner(SpireStageRunner):
    params = {
        "task_name": constants.POSTPROCESSING,
        "run_name": constants.POSTPROCESSING_RUN,
        "cluster_type": constants.POSTPROCESSING_CLUSTER_TYPE,
        "libraries": constants.DEPENDENCIES,
        "task_type": constants.POSTPROCESSING_TASK_TYPE,
        "runner_script": constants.POSTPROCESSING_RUNNER_SCRIPT,
        "wheels": constants.WHEEL_DEPS,
        "timeout_seconds": constants.POSTPROCESSING_TIMEOUT_SECONDS,
        "docker_image": constants.DOCKER_IMAGE_CONFIG,
    }

    def __init__(self, **kwargs):
        default_params = PostProcessingRunner.params.copy()
        default_params.update(kwargs)

        super(PostProcessingRunner, self).__init__(**default_params)
