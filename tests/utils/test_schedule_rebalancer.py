import datetime
from typing import List
from unittest.mock import MagicMock
from spire.framework.workflows.schedule_rebalancer import (
    ScheduleRebalancer,
    ScheduleBlock,
)
from spire.framework.workflows import Workflow
from spire.config import config


# TODO(Max): The parts of this script which are mocked should be remade as
# integrations tests where e.g. workflows are created as fixtures, and the
# rebalancer algorithm should then be turned into unit-tests given assumptions
# determined from the integrations tests.


def create_balanced_workflows() -> List[Workflow]:
    workflows = []
    start_date = datetime.date(2020, 9, 2)
    rb = ScheduleRebalancer(ENV=config.DEPLOYMENT_ENV, START_DATE=start_date)
    for i in range(200):
        workflows.append(Workflow(name=f"spire_test_{i}", description=""))
    session = MagicMock()
    rb.rebalance_workflows(workflows, session=session)
    return workflows


def test_rebalance():
    workflows = create_balanced_workflows()
    vendor = "test"
    start_date = datetime.date(2020, 9, 2)
    rb = ScheduleRebalancer(ENV=config.DEPLOYMENT_ENV, START_DATE=start_date)
    rb.vendor = vendor
    rb._get_schedule_counts(workflows)
    day_counts = rb.day_counts
    expected_num_blocks = 10
    assert len(workflows) == sum(rb.blocks.values()) == sum(day_counts)
    assert len(rb.blocks) == expected_num_blocks
    blocks_fixture = [
        f"0 0 * * 0, {rb.START_DATE}",
        f"0 1 * * 0, {rb.START_DATE}",
        f"0 2 * * 0, {rb.START_DATE}",
        f"0 0 * * 1, {rb.START_DATE}",
        f"0 1 * * 1, {rb.START_DATE}",
        f"0 2 * * 1, {rb.START_DATE}",
        f"0 0 * * 2, {rb.START_DATE}",
        f"0 1 * * 2, {rb.START_DATE}",
        f"0 2 * * 2, {rb.START_DATE}",
        f"0 0 * * 3, {rb.START_DATE}",
    ]
    for block in rb.blocks:
        assert block.block_str in blocks_fixture


def test_multiple_minimums_next_hour():
    # setup
    start_date = datetime.date(2020, 9, 2)
    rb = ScheduleRebalancer(ENV=config.DEPLOYMENT_ENV, START_DATE=start_date)
    rb.vendor = "test"
    test = {
        ScheduleBlock(f"0 0 * * 0, {rb.START_DATE}"): 20,
        ScheduleBlock(f"0 1 * * 0, {rb.START_DATE}"): 10,
        ScheduleBlock(f"0 0 * * 1, {rb.START_DATE}"): 10,
    }
    rb.blocks = test
    test_cron_interval = "0 1 * * 0"

    # test
    sch = rb.create_new_balanced_schedule()

    # assert
    assert sch.schedules["assembly"].cron_interval == test_cron_interval


def test_multiple_minimums_next_day():
    # setup
    start_date = datetime.date(2020, 9, 2)
    rb = ScheduleRebalancer(ENV=config.DEPLOYMENT_ENV, START_DATE=start_date)
    rb.vendor = "test"
    test = {
        ScheduleBlock(f"0 0 * * 0, {rb.START_DATE}"): 20,
        ScheduleBlock(f"0 0 * * 1, {rb.START_DATE}"): 10,
        ScheduleBlock(f"0 1 * * 1, {rb.START_DATE}"): 10,
    }
    rb.blocks = test
    test_cron_interval = "0 0 * * 1"

    # test
    sch = rb.create_new_balanced_schedule()

    # assert
    assert sch.schedules["assembly"].cron_interval == test_cron_interval


def test_no_viable_create_next_hour():
    # setup
    start_date = datetime.date(2020, 9, 2)
    rb = ScheduleRebalancer(ENV=config.DEPLOYMENT_ENV, START_DATE=start_date)
    rb.vendor = "test"
    test = {
        ScheduleBlock(f"0 0 * * 0, {rb.START_DATE}"): 20,
        ScheduleBlock(f"0 1 * * 0, {rb.START_DATE}"): 20,
    }
    rb.blocks = test
    test_cron_interval = "0 2 * * 0"

    # test
    sch = rb.create_new_balanced_schedule()

    # assert
    assert sch.schedules["assembly"].cron_interval == test_cron_interval


def test_no_viable_create_next_date():
    # setup
    start_date = datetime.date(2020, 9, 2)
    rb = ScheduleRebalancer(ENV=config.DEPLOYMENT_ENV, START_DATE=start_date)
    rb.vendor = "test"
    test = {
        ScheduleBlock(f"0 0 * * 0, {rb.START_DATE}"): 20,
        ScheduleBlock(f"0 1 * * 0, {rb.START_DATE}"): 20,
        ScheduleBlock(f"0 2 * * 0, {rb.START_DATE}"): 20,
    }
    rb.blocks = test
    test_cron_interval = "0 0 * * 1"

    # test
    sch = rb.create_new_balanced_schedule()

    # assert
    assert sch.schedules["assembly"].cron_interval == test_cron_interval
