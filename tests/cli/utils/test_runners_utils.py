import pytest
import datetime
from cli.utils import runners_utils as utils


def test_get_dates_single():
    run_date = "2020-01-12"
    days_to_backfill = 1
    dates = utils.get_dates(run_date=run_date, days_to_backfill=days_to_backfill)
    assert len(dates) == 1
    assert dates[0].strftime("%Y-%m-%d") == run_date


def test_get_dates_backfill():
    run_date = "2020-01-12"
    days_to_backfill = 3
    dates_fixture = [
        datetime.date(2020, 1, 12),
        datetime.date(2020, 1, 11),
        datetime.date(2020, 1, 10),
    ]
    dates = utils.get_dates(run_date=run_date, days_to_backfill=days_to_backfill)
    assert len(dates) == 3
    assert dates == dates_fixture


def test_get_dates_rundate_specific_dates():
    run_date = "2020-01-12"
    days_to_backfill = 1
    specific_dates = "2020-02-12,2020-03-12,2020-04-12"
    with pytest.raises(Exception) as e:
        utils.get_dates(
            run_date=run_date,
            days_to_backfill=days_to_backfill,
            specific_dates=specific_dates,
        )
        message = "the arguments run-date and specific-dates can't be used together"
        assert str(e.value) == message


def test_get_dates_specificdates():
    specific_dates = "2020-02-12,2020-03-12,2020-04-12"
    dates_fixture = [
        datetime.date(2020, 2, 12),
        datetime.date(2020, 3, 12),
        datetime.date(2020, 4, 12),
    ]
    dates = utils.get_dates(specific_dates=specific_dates)
    assert len(dates) == 3
    assert dates == dates_fixture


def test_get_list_workflow_ids_filepath():
    wf_ids = "foo,bar,baz"
    filepath = "foo/bar/baz.csv"
    with pytest.raises(Exception) as e:
        utils.get_list_workflow_ids(wf_ids=wf_ids, filepath=filepath)
        message = "the arguments wf-ids and filepath can't be used together"
        assert str(e.value) == message
