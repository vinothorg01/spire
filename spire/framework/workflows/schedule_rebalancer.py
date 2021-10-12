import math
import copy
import datetime
from collections import Counter
from typing import TYPE_CHECKING, List, Dict, Any

from sqlalchemy.orm.session import Session

from spire.utils.general import group_by_vendor
from spire.utils.logger import get_logger
from spire.framework.workflows.schedule import (
    ScheduledStages,
    CronSchedule,
    StageSchedule,
)
from spire.integrations.postgres import connector


if TYPE_CHECKING:
    # We need Workflow import for type hinting, but there would otherwise be a
    # circular import issue since schedule and rebalancer are imported into workflow.py
    from spire.framework.workflows import Workflow


logger = get_logger(__name__)

utc_est_offset = 6


class ScheduleRebalancer:
    def __init__(self, **kwargs):
        """
        Rebalancing is an approach to organize how workflows are scheduled to
        assemble and train based on the logics described below.

        Currently, we schedule all workflows to score daily, and the training schedules
        are dependent on assembly, so rebalancing is designed to assume assembly and
        training are run together, and gives scoring a daily schedule, but any
        workflow's schedule can be adjusted manually.

        A ScheduleRebalancer instance defaults with the following
        attributes, which can be modified post-hoc:

        DAYS_PER_WEEK: (7) Days in a week for scheduling (starting from 1)

        HOURS_PER_DAY: (24) Number of hours in a day for scheduling
        (starting from 1)

        DAY_BLOCK_SIZE: (60) Number of workflows per group per day that can
        be scheduled together

        HOUR_BLOCK_SIZE: (20) Number of workflows per group per hour that can
        be scheduled together

        START_DATE: (yesterday) Date to begin with if rebalancing the whole schedule

        vendor: Defined by _get_schedule_counts method. The vendor for the new schedule
        for assembly/training

        blocks: Defined by _get_schedule_counts method. A dictionary of ScheduleBlock
        objects as keys, and counts per block as values
        """
        self.DAYS_PER_WEEK = kwargs.get("DAYS_PER_WEEK", 7)
        self.HOURS_PER_DAY = kwargs.get("HOURS_PER_DAY", 24)
        self.DAY_BLOCK_SIZE = kwargs.get("DAY_BLOCK_SIZE", 60)
        self.HOUR_BLOCK_SIZE = kwargs.get("HOUR_BLOCK_SIZE", 20)
        self.START_DATE = kwargs.get(
            "START_DATE", datetime.datetime.today() - datetime.timedelta(days=1)
        )
        self.vendor = None
        self.blocks = None

    def _get_schedule_counts(self, workflows: List["Workflow"]):
        """
        Takes a list of workflows, groups them by vendor, and creates a
        ScheduleBlocks: counts dictionary attribute on the rebalancer
        instance, as well as a vendor attribute
        """
        grouped = group_by_vendor(workflows)[self.vendor]
        schedules = []
        for wf in grouped:
            try:
                sch = wf.schedule.schedules["assembly"]
                cron_interval = sch.cron_interval.split(" ")
                hour_day_flag = (
                    True
                    if cron_interval[1].isdigit() and cron_interval[4].isdigit()
                    else False
                )
                if hour_day_flag:
                    block_str = (
                        f"{sch.cron_interval}, {sch.start_date.strftime('%Y-%m-%d')}"
                    )
                    schedules.append(block_str)
            except Exception as e:
                logger.error(str(e))
        self.blocks = Counter(schedules)
        schedule_blocks = {block: ScheduleBlock(block) for block in self.blocks}
        self.blocks = {
            schedule_blocks[block]: count for block, count in self.blocks.items()
        }

    def _count_blocks_by_day(self, day: int) -> int:
        """
        Loop over blocks attribute, and where a block's day is equal to the inputted
        day, add those counts to the running counts and return the count
        """
        count = 0
        for block, count_ in self.blocks.items():
            if block.day == day:
                count += count_
        return count

    @property
    def day_counts(self) -> List[int]:
        """
        From the blocks attribute, automatically reformat the dictionary into a list of
        counts across each day of the week and returns the list. The ScheduleBlocks
        Counter dictionary is produced with the _get_schedule_counts method
        """
        return [self._count_blocks_by_day(day) for day in range(self.DAYS_PER_WEEK)]

    @connector.session_transaction
    def rebalance_workflows(self, workflows: List["Workflow"], session: Session = None):
        """
        Rebalances all workflows passed to the method, given
        the attributes of the rebalancer instance
        """
        grouped = group_by_vendor(workflows)
        for group in grouped:
            wfs = grouped[group]
            schedules = self.rebalance(wfs)
            for wf in wfs:
                schedule = schedules[wf]
                session.add(schedule)
                wf.schedule = schedule

    def rebalance(
        self, workflows: List["Workflow"]
    ) -> Dict["Workflow", Dict[str, ScheduledStages]]:
        """
        Finds and creates schedules given the parameters on the
        rebalancer instances and returns a dictionary pairing each
        workflow with a schedule
        """
        num_workflows = len(workflows)
        blocks = self.define_schedule_blocks(num_workflows)
        workflow_schedules_zipped = dict(zip(workflows, blocks))
        return {
            wf: self.create_schedule(block.day, block.hour)
            for wf, block in workflow_schedules_zipped.items()
        }

    def define_schedule_blocks(self, num_workflows: int) -> List["ScheduleBlock"]:
        per_day = self.define_day_block(num_workflows)
        per_hour = self.define_hour_block(per_day)
        start_date = self.START_DATE.strftime("%Y-%m-%d")
        blocks = []
        for i in range(num_workflows):
            day = i // per_day
            hour = (i % per_day) // per_hour
            block_str = f"0 {hour} * * {day}, {start_date}"
            blocks.append(ScheduleBlock(block_str))
        return blocks

    def define_day_block(self, num_workflows: int) -> int:
        return self.DAY_BLOCK_SIZE * math.ceil(
            (num_workflows / self.DAY_BLOCK_SIZE) / self.DAYS_PER_WEEK
        )

    def define_hour_block(self, per_day: int) -> int:
        return self.HOUR_BLOCK_SIZE * math.ceil(
            (per_day / self.HOUR_BLOCK_SIZE) / self.HOURS_PER_DAY
        )

    def create_schedule(self, day: int, hour: int) -> ScheduledStages:
        return ScheduledStages(
            {
                "assembly": CronSchedule(f"0 {hour} * * {day}", self.START_DATE),
                "training": CronSchedule(f"0 {hour} * * {day}", self.START_DATE),
                "scoring": CronSchedule(f"0 {utc_est_offset} * * *", self.START_DATE),
            }
        )

    def create_new_balanced_schedule(self) -> ScheduledStages:
        f"""
        Returns a StageSchedule object which fits within the current balance of workflow
        schedules, so that a workflow can be added to the schedule without breaking the
        balance. This method assumes that the blocks parameter be defined on the
        rebalancer instance, such as by _get_schedule_counts, or else it will create a
        new schedule with day=0 and hour={utc_est_offset} and throw a warning
        """
        if not self.blocks:
            logger.warning(
                f"Counts were not provided, so a new schedule block is being created "
                f"where day=0 and hour={utc_est_offset}. Is this part of an "
                f"integrations test?"
            )
            return self.create_schedule(day=0, hour=utc_est_offset)
        num_blocks = sum(list(self.blocks.values()))
        schedules_per_day = self.define_day_block(num_blocks)
        schedules_per_hour = self.define_hour_block(schedules_per_day)
        schedule_params = {
            "blocks": self.blocks,
            "num_blocks": num_blocks,
            "per_day": schedules_per_day,
            "per_hour": schedules_per_hour,
            "day_counts": self.day_counts,
        }
        return self._schedule_helper(schedule_params)

    def _schedule_helper(
        self, schedule_params: Dict[str, Any], blocks: Dict["ScheduleBlock", int] = None
    ) -> ScheduledStages:
        """
        Helper method to create a new StageSchedule object of CronSchedules that
        fits within the current schedule balance
        """
        blocks = copy.deepcopy(blocks or schedule_params["blocks"])
        block = min(blocks, key=blocks.get)
        count = blocks[block]
        schedule = self.create_schedule(str(block.day), str(block.hour))
        if block.check_block_capacity(schedule_params, count):
            blocks.pop(block)
            schedule = self._shift_schedule(blocks, block, schedule_params)
        return schedule

    def _shift_schedule(
        self,
        blocks: Dict["ScheduleBlock", int],
        block: "ScheduleBlock",
        params: Dict[str, Any],
    ) -> ScheduledStages:
        """
        Determines whether there is an available block for the next hour
        or the next day, and shifts the block's schedule over if so
        """
        if not blocks:
            if block.check_next_hour(self, params):
                schedule = block.assign_next_hour(self)
            elif block.check_next_day(self):
                schedule = block.assign_next_day(self)
            else:
                message = (
                    "There are no more available blocks. "
                    "Increase day_block_size or hour_block_size "
                    "and rebalance schedules."
                )
                logger.error(message)
                raise Exception(message)
        else:
            schedule = self._schedule_helper(params, blocks=blocks)
        return schedule


class ScheduleBlock:
    """
    The block_str is a string containing the cron_code and start date
    for a given schedule block, i.e. '0 1 * * 0, 2020-12-13'. A
    ScheduleBlock instance has attributes for the  block_str, cron_code
    (i.e. '0 1 * * 0'), start_date, day of week and hour from the cron_code
    (e.g. 0, *, */2), and creates a CronSchedule object.

    It also contains methods for the logic for where to add a new workflow
    into a pre-balanced set of schedule blocks.
    """

    def __init__(self, block_str: str):
        self.block_str = block_str
        self.cron_code = block_str.split(", ")[0]
        self.start_date = block_str.split(", ")[1]
        self.day = int(self.cron_code.split(" ")[4])
        self.hour = int(self.cron_code.split(" ")[1])

    def check_block_capacity(self, schedule_params: Dict[str, Any], count: int) -> bool:
        """
        Return True if the count of number of blocks per hour
        has been exceeded or the count of number of blocks per
        day has been exceeded
        """
        return (
            count == schedule_params["per_hour"]
            or schedule_params["day_counts"][self.day] == schedule_params["per_day"]
        )

    def check_next_hour(
        self, rb: ScheduleRebalancer, schedule_params: Dict[str, Any]
    ) -> bool:
        """
        Return True if there is another available hour in the day
        and the count of number of blocks per day has not been exceeded
        """
        return (
            self.hour < (rb.HOURS_PER_DAY - 1)
            and schedule_params["day_counts"][self.day] < schedule_params["per_day"]
        )

    def check_next_day(self, rb: ScheduleRebalancer) -> bool:
        """
        Return True if there is another available day in the week
        """
        return self.day < (rb.DAYS_PER_WEEK - 1)

    def assign_next_hour(self, rb: ScheduleRebalancer) -> StageSchedule:
        logger.info(
            "The previous most valid schedule block hour for this group has "
            "reached capacity. The next valid schedule block hour will be "
            "chosen instead."
        )
        next_hour = f"{self.hour + 1}"
        return rb.create_schedule(str(self.day), next_hour)

    def assign_next_day(self, rb: ScheduleRebalancer) -> StageSchedule:
        logger.info(
            "The previous most valid schedule block day for this group has "
            "reached capacity for all hours. The next valid schedule block day "
            " will be chosen instead, starting from hour 0."
        )
        next_day = f"{self.day + 1}"
        return rb.create_schedule(next_day, "0")
