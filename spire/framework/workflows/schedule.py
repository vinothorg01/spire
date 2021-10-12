import time
import copy
import datetime
import croniter

from sqlalchemy.orm import relationship
from sqlalchemy import orm, cast, String, case

from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.types import JSON

from spire.framework.workflows.connectors import Base
from spire.framework.workflows.constants import (
    MAX_NUM_TASK_RETRIES,
)
from spire.framework.workflows.workflow_stages import WorkflowStages

from spire.integrations.postgres import connector
from spire.utils import assert_datetime_argument
from spire.utils.logger import get_logger

logger = get_logger()


class Schedule(Base):
    __tablename__ = "schedules"
    workflow_id = Column(
        UUID(as_uuid=True),
        ForeignKey("workflows.id", ondelete="CASCADE"),
        primary_key=True,
    )
    definition = Column(JSON, nullable=True)
    workflow = relationship(
        "Workflow", uselist=False, single_parent=True, back_populates="schedule"
    )

    __mapper_args__ = {
        "polymorphic_identity": "schedule",
        # if we assign `definition["logic"]` here, SQLAlchemy will sometimes
        # failed to lookup the schedule for the workflow because of type mismatch.
        # This fixes that by casting `definition["logic"]` to string.
        # Note that if another scheduling logic got implemented, we'll have to
        # add another case to the case statement.
        "polymorphic_on": case(
            [
                (
                    cast(definition["logic"], String) == "scheduled_stages",
                    "scheduled_stages",
                )
            ],
            else_="scheduled_stages",
        ),
        "with_polymorphic": "*",
    }

    def __init__(self):
        self.definition = self.to_dict()

    def ready_to_run(self, target_date, stage):
        raise Exception(
            "Class {} has no implementation for 'ready_to_run'".format(
                self.__class__.__name__
            )
        )

    def to_dict(self):
        raise Exception(
            "Class {} has no implementation for 'to_dict'".format(
                self.__class__.__name__
            )
        )

    @classmethod
    def from_dict(cls, d):
        return SCHEDULE_TYPES[d["logic"]].from_dict(d)

    def init_from_dict(self, d):
        raise Exception(
            "Class {} has no implementation for 'init_from_dict'".format(
                self.__class__.__name__
            )
        )

    @orm.reconstructor
    def init_on_load(self):
        try:
            self.init_from_dict(self.definition)
        except Exception as e:
            print(self.__class__.__name__)
            print("warning -- init from dict failed with error : {}".format(e))


class ScheduledStages(Schedule):
    __mapper_args__ = {
        "polymorphic_load": "inline",
        "polymorphic_identity": "scheduled_stages",
    }

    def __init__(self, schedules):
        self.schedules = schedules
        for v in self.schedules.values():
            v.update_owner(self)
        super(ScheduledStages, self).__init__()

    def ready_to_run(self, stage: str, target_date):
        # TODO: convert stage to enum
        if stage not in [s.value for s in WorkflowStages]:
            raise Exception(
                """stage must be one of "
                            "['assembly', 'training',
                            'scoring', 'postprocessing']"""
            )
        if stage not in self.schedules.keys():
            return False
        return self.schedules[stage].ready_to_run(target_date)

    @classmethod
    def from_dict(cls, d):
        schedules = {
            stage: StageSchedule.from_dict(schedule_dict)
            for stage, schedule_dict in d["args"].items()
        }
        return cls(schedules)

    def to_dict(self):
        return {
            "logic": "scheduled_stages",
            "args": {
                stage: schedule.to_dict() for stage, schedule in self.schedules.items()
            },
        }

    def init_from_dict(self, d):
        copied_dict = copy.deepcopy(d)
        self.schedules = {
            stage: StageSchedule.from_dict(schedule_dict)
            for stage, schedule_dict in copied_dict["args"].items()
        }
        for v in self.schedules.values():
            v.update_owner(self)


SCHEDULE_TYPES = {"scheduled_stages": ScheduledStages}


class StageSchedule:
    def __init__(self):
        pass

    def ready_to_run(self, target_date):
        raise Exception(
            "Class {} has no implementation for 'ready_to_run'".format(
                self.__class__.__name__
            )
        )

    def to_dict(self):
        raise Exception(
            "Class {} has no implementation for 'ready_to_run'".format(
                self.__class__.__name__
            )
        )

    @classmethod
    def from_dict(cls, d):
        schedule_instance = STAGE_SCHEDULE_TYPES[d["logic"]].from_dict(d)
        return schedule_instance

    def update_owner(self, owner):
        self.owner = owner


class CronSchedule(StageSchedule):
    def __init__(self, cron_interval, start_date):
        super(CronSchedule, self).__init__()
        self.cron_interval = cron_interval
        if isinstance(start_date, datetime.date):
            start_date = datetime.datetime.combine(
                start_date, datetime.datetime.min.time()
            )
        self.start_date = start_date

    @classmethod
    def from_dict(cls, schedule_dict):
        if schedule_dict is None:
            return cls("@never", "2100-01-01")
        start_date = datetime.datetime.strptime(
            schedule_dict["args"]["start_date"], "%Y-%m-%d"
        ).date()
        return cls(schedule_dict["args"]["cron_interval"], start_date)

    def to_dict(self):
        if self.cron_interval == "@never":
            raise Exception("No set schedule exists for this workflow")
        else:
            return {
                "logic": "cron",
                "args": {
                    "cron_interval": self.cron_interval,
                    "start_date": self.start_date.strftime("%Y-%m-%d"),
                },
            }

    def ready_to_run(self, target_date):
        if self.cron_interval == "@never":
            return False
        start = time.time()
        target_date_trunc = assert_datetime_argument(target_date)
        if not isinstance(target_date, datetime.datetime):
            raise Exception("target_date must be a datetime object.")
        cron_hour = target_date.hour
        cron_min = target_date.minute
        target_date_trunc = target_date_trunc.replace(hour=cron_hour, minute=cron_min)
        base_date = croniter.croniter(self.cron_interval, self.start_date)
        next_date = base_date.get_next(datetime.datetime)
        while next_date <= target_date_trunc:
            now = time.time()
            if int(now - start) >= 5:
                raise TimeoutError("Scheduling timed out for this workflow")
            if next_date == target_date_trunc:
                return True
            else:
                next_date = base_date.get_next(datetime.datetime)
        return False


class ExpirationSchedule(StageSchedule):
    """Schedule based on the expiration_date"""

    def __init__(self, expiration_date: datetime.date = None):
        super(ExpirationSchedule, self).__init__()
        self.expiration_date = expiration_date
        if self.expiration_date is None:
            self.expiration_date = datetime.date(1970, 1, 1)

    @classmethod
    def from_dict(cls, schedule_dict):
        if schedule_dict is None:
            # default to expired
            return cls(datetime.date(1970, 1, 1))

        expiration_date = datetime.datetime.strptime(
            schedule_dict["args"]["expiration_date"], "%Y-%m-%d"
        ).date()
        return cls(expiration_date)

    def to_dict(self):
        return {
            "logic": "expiration",
            "args": {"expiration_date": self.expiration_date.strftime("%Y-%m-%d")},
        }

    def ready_to_run(self, target_date: datetime.date):
        if isinstance(target_date, datetime.datetime):
            target_date = target_date.date()
        return target_date >= self.expiration_date

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.expiration_date == other.expiration_date
        return False


class CountdownSchedule(StageSchedule):
    def __init__(self, stage, seconds_until_stale, n_tries=3):
        super(CountdownSchedule, self).__init__()
        self.stage = stage
        self.seconds_until_stale = seconds_until_stale
        self.n_tries = n_tries

    @classmethod
    def from_dict(cls, schedule_dict):
        stage = schedule_dict["args"]["stage"]
        seconds_until_stale = schedule_dict["args"]["seconds_until_stale"]
        n_tries = schedule_dict["args"]["n_tries"]
        return cls(stage, seconds_until_stale, n_tries)

    def to_dict(self):
        return {
            "logic": "countdown",
            "args": {
                "stage": self.stage,
                "seconds_until_stale": self.seconds_until_stale,
                "n_tries": self.n_tries,
            },
        }

    def ready_to_run(self, target_date):
        target_date = assert_datetime_argument(target_date)
        session = connector.make_session()
        try:
            if self.stage == WorkflowStages.TRAINING.value:
                ready_upstream = self.__check_upstream_stage(session, target_date)
            else:
                ready_upstream = True

            excess_failures = self.__check_excessive_failures(session)
            stale = self.__check_for_staleness(session, target_date)
            if not excess_failures and all([ready_upstream, stale]):
                return True
        except Exception as e:
            session.rollback()
            raise (e)
        finally:
            session.close()
        return False

    def __check_upstream_stage(self, session, target_date):
        dependent_stage = WorkflowStages.ASSEMBLY.value
        upstream_last_run = self.owner.workflow.last_successful_run_date(
            session, dependent_stage
        )
        if not upstream_last_run:
            print("no training set assembled for wf {}".format(self.owner.workflow.id))
            return False
        seconds_since_last_upstream_success = (
            target_date - upstream_last_run
        ).total_seconds()
        if seconds_since_last_upstream_success > self.seconds_until_stale:
            print("old training set for wf {}".format(self.owner.workflow.id))
            return False
        return True

    def __check_excessive_failures(self, session):
        n_failures = self.owner.workflow.get_recent_failures(
            session, self.stage, self.seconds_until_stale
        )
        if len(n_failures) >= MAX_NUM_TASK_RETRIES:
            logger.warning(
                "too many failures for wf {}!".format(self.owner.workflow.id)
            )
            print("too many failures for wf {}!".format(self.owner.workflow.id))
            return True
        return False

    def __check_for_staleness(self, session, target_date):
        date_last_run = self.owner.workflow.last_successful_run_date(
            session, self.stage
        )
        if not date_last_run:
            return True
        seconds_since_last_run = (target_date - date_last_run).total_seconds()
        if seconds_since_last_run >= self.seconds_until_stale:
            return True
        return False


STAGE_SCHEDULE_TYPES = {
    "cron": CronSchedule,
    "countdown": CountdownSchedule,
    "expiration": ExpirationSchedule,
}
