# allow classmethod to return its own class, can remove in 3.10
from __future__ import annotations

from typing import List
from abc import ABC, abstractmethod
from enum import Enum
from pydantic import BaseModel, Extra
import sys
from spire.utils.logger import get_logger
from spire.utils import constants
import time

logger = get_logger(__name__)


class JobStatuses(str, Enum):
    # TODO(Max): It is apparently not possible, and also a conceptual violation, to
    # extend enum classes, so probably we should refactor this from a base class to a
    # set of task-specific classes.
    # NOTE(Max): ScoringError (e.g. MLFLOW_DEPENDENCE_ERROR, MISSING_FEATURES_ERROR) is
    # custom for Scoring, see TODO above.
    PENDING = "pending"
    RUNNING = "running"
    FAILED = "failed"
    SUCCESS = "success"
    # This allows us to differentiate between workflows that failed to score for
    # miscellaneous reasons, vs. those that failed specifically because they were
    # missing mlflow information, meaning they were likely not trained. This can be
    # used by the reporter to give us more usefully granular reports.
    MLFLOW_DEPENDENCE_ERROR = "MLFlowDependenceError"
    MISSING_FEATURES_ERROR = "MissingFeaturesError"
    MISSING_HISTORY_ERROR = "MissingHistoryError"

    def __str__(self) -> str:
        """
        Use the value as this Enum string representation for easy transition
        from string based value to Enum based.

        e.g.
        str(JobStatuses.SUCCESS) == str('success')
        """

        return self.value


class Job(ABC, BaseModel):
    """A dataclass to store everything that is required to run the a job."""

    class Config:
        extra = Extra.forbid

    def __init__(self, **data) -> None:
        # We need to override __init__ to add private attribute, otherwise
        # we won't be able to set it.
        super().__init__(**data)

    def get_runner_name(self):
        return self.__class__.__name__ + "Runner"


class JobStatus(ABC):
    """Implement logic to retrieve the job status"""

    def __init__(self, job: Job) -> None:
        self.job: Job = job
        self.latest_status: JobStatuses = None

    @abstractmethod
    def _get_status(self) -> JobStatuses:
        pass

    def get_status(self) -> JobStatuses:
        new_status = self._get_status()
        if new_status != self.latest_status:
            logger.info(
                f"{self} transitioned from {self.latest_status} to {new_status}"
            )
        self.latest_status = new_status
        return self.latest_status

    @staticmethod
    def _remove_finished_jobs(job_statuses: List[JobStatus]) -> List[JobStatus]:
        return list(
            filter(
                lambda s: s.latest_status in [JobStatuses.PENDING, JobStatuses.RUNNING],
                job_statuses,
            )
        )

    @staticmethod
    def monitor_statuses(
        statuses: List[JobStatus], interval=constants.POLLING_PERIOD_INTERVAL
    ) -> None:
        """Check and log status of jobs

        Args:
            interval: Polling interval in seconds
        """
        assert isinstance(statuses, list)
        remaining_jobs = statuses
        while len(remaining_jobs) > 0:
            logger.info(f"Checking {len(remaining_jobs)} job statuses")
            for status in remaining_jobs:
                # update status
                status.get_status()

                # log status
                logger.info(str(status))

            remaining_jobs = JobStatus._remove_finished_jobs(remaining_jobs)
            time.sleep(interval)


class JobRunner(ABC):
    """Responsible for knowing how to run an instance of a Job"""

    def __init__(self, job) -> None:
        super().__init__()
        self.job = job

    @abstractmethod
    def run(self) -> JobStatus:
        pass

    @classmethod
    def get_runner_for_job(cls, job) -> JobRunner:
        """Return the correct JobRunner for the supplied job"""
        runner_name = job.get_runner_name()
        reg = sys.modules["spire.framework.execution.implementations"]
        try:
            runner = getattr(reg, runner_name)
        except KeyError as e:
            logger.error(
                f"{runner_name} not found in"
                "`spire.framework.execution.implementations`, please import"
                " them first in  implementations/__init__.py and try again."
            )
            raise e

        assert issubclass(runner, JobRunner)
        return runner(job)
