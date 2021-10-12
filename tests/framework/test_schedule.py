import pytest
import datetime

from spire.framework.workflows.schedule import (
    ScheduledStages,
    CronSchedule,
    ExpirationSchedule,
)


def test_cron_stages():
    # setup
    start_date = datetime.datetime.today()

    # exercise
    schedule = ScheduledStages(
        {
            "assembly": CronSchedule("0 0 * * 0", start_date),
            "training": CronSchedule("2 0 * * 1", start_date),
            "scoring": CronSchedule("0 0 * * *", start_date),
        }
    )

    # assert
    assert "assembly" in schedule.schedules
    assert "training" in schedule.schedules
    assert "scoring" in schedule.schedules


def test_cron_intervals():
    # setup
    start_date = datetime.datetime.today()
    assembly = "0 0 * * 0"
    training = "2 0 * * 1"
    scoring = "0 0 * * *"

    # exercise
    schedule = ScheduledStages(
        {
            "assembly": CronSchedule(assembly, start_date),
            "training": CronSchedule(training, start_date),
            "scoring": CronSchedule(scoring, start_date),
        }
    )

    # assert
    assert schedule.schedules["assembly"].cron_interval == assembly
    assert schedule.schedules["training"].cron_interval == training
    assert schedule.schedules["scoring"].cron_interval == scoring


def test_ready_to_run_hour():
    # setup
    start_date = datetime.datetime(2020, 7, 28, 8, 0, 0)
    target_date = datetime.datetime(2020, 8, 2, 8, 0, 0)

    # exercise
    schedule = ScheduledStages(
        {
            "assembly0": CronSchedule("0 0 * * 0", start_date),
            "assembly4": CronSchedule("0 4 * * 0", start_date),
            "assembly8": CronSchedule("0 8 * * 0", start_date),
            "assembly12": CronSchedule("0 12 * * 0", start_date),
            "assembly8_1": CronSchedule("0 8 * * 1", start_date),
        }
    )

    # assert
    assert not schedule.schedules["assembly0"].ready_to_run(target_date)
    assert not schedule.schedules["assembly4"].ready_to_run(target_date)
    assert schedule.schedules["assembly8"].ready_to_run(target_date)
    assert not schedule.schedules["assembly12"].ready_to_run(target_date)
    assert not schedule.schedules["assembly8_1"].ready_to_run(target_date)


def test_ready_to_run_require_datetime():
    # setup
    start_date = datetime.datetime(2020, 7, 28, 8, 0, 0)
    target_date = datetime.datetime(2020, 8, 2, 8, 0, 0).date()

    # exercise
    schedule = ScheduledStages(
        {
            "assembly": CronSchedule("0 0 * * 0", start_date),
            "training": CronSchedule("2 0 * * 1", start_date),
            "scoring": CronSchedule("0 0 * * *", start_date),
        }
    )

    # assert
    with pytest.raises(Exception):
        schedule.schedules["assembly"].ready_to_run(target_date)


def test_expiration_schedule():
    target_date = datetime.datetime(2020, 8, 2, 8, 0, 0).date()
    after_target = target_date + datetime.timedelta(days=1)
    before_target = target_date - datetime.timedelta(days=1)

    expired = ExpirationSchedule(before_target)
    not_expired = ExpirationSchedule(after_target)

    assert expired.ready_to_run(target_date) == True
    assert not_expired.ready_to_run(target_date) == False


def test_expiration_schedule_serialization():
    target_date = datetime.datetime(2020, 8, 2, 8, 0, 0).date()
    before_target = target_date - datetime.timedelta(days=1)
    expired = ExpirationSchedule(before_target)
    assert expired == ExpirationSchedule.from_dict(expired.to_dict())
