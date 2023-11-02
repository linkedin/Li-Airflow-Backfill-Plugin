from airflow.utils import timezone
from datetime import timedelta

from linkedin.airflow.backfill.utils.backfill_util import (
    parse_backfill_deleted,
    is_backfill_permitted,
    is_any_logical_date_in_range,
    try_json_to_dict,
    dict_get_func,
    object_get_func,
)
from tests.linkedin.airflow.backfill.test_models import get_random_backfill
from tests.linkedin.airflow.backfill.testutils import TestObj


def test_parse_backfill_deleted():
    o = get_random_backfill()
    curr_dt = timezone.utcnow()
    # 1. deleted
    o.deleted = True
    assert parse_backfill_deleted(o, curr_dt) == "Deleted"

    # mark deleted
    o.deleted = False
    o.delete_date = curr_dt - timedelta(hours=1)
    assert parse_backfill_deleted(o, curr_dt) == "ToBeDeleted"

    # active
    o.deleted = False
    o.delete_date = curr_dt + timedelta(hours=2)
    assert parse_backfill_deleted(o, curr_dt) == ""


def test_is_backfill_permitted():
    o = get_random_backfill()

    # 1. deleted
    o.deleted = True
    permissted_dag_ids = [o.dag_id]
    assert is_backfill_permitted(o, permissted_dag_ids) is True

    # active
    o.deleted = False
    permissted_dag_ids = [o.backfill_dag_id]
    assert is_backfill_permitted(o, permissted_dag_ids) is True


def test_is_any_logical_date_in_range():
    curr_dt = timezone.utcnow()
    start_date = curr_dt - timedelta(hours=3)
    end_date = curr_dt

    # 1. no logical date in range
    logical_dates = [curr_dt - timedelta(hours=5), curr_dt + timedelta(hours=5)]
    assert is_any_logical_date_in_range(logical_dates, start_date, end_date) is False

    # date on the edge
    logical_dates = [curr_dt - timedelta(hours=5), start_date]
    assert is_any_logical_date_in_range(logical_dates, start_date, end_date) is True
    logical_dates = [curr_dt - timedelta(hours=5), end_date]
    assert is_any_logical_date_in_range(logical_dates, start_date, end_date) is True

    # date in range
    logical_dates = [curr_dt - timedelta(hours=5), curr_dt - timedelta(hours=1)]
    assert is_any_logical_date_in_range(logical_dates, start_date, end_date) is True


def test_try_json_to_dict():
    # 1. valid
    json_str = '{"key1": "value1", "key2": "value2"}'
    r, err = try_json_to_dict(json_str)
    assert err == ""
    assert r["key1"] == "value1"
    assert r["key2"] == "value2"

    # 2. non dict json
    json_str = '["value1", "value2"]'
    r, err = try_json_to_dict(json_str)
    assert err != ""
    assert r is None

    # 3. invalid json
    json_str = '{"key1": "value1"'
    r, err = try_json_to_dict(json_str)
    assert err != ""
    assert r is None


def test_dict_get_func():
    val = {
        "key1": "value1",
        "key2": "value2",
    }

    # 1. no default
    assert dict_get_func(val, "key1") == "value1"
    assert dict_get_func(val, "key3") is None

    # 2. with default
    assert dict_get_func(val, "key1", "default") == "value1"
    assert dict_get_func(val, "key3", "default") == "default"


def test_object_get_func():
    val = TestObj(key1="value1", key2="value2")

    # 1. no default
    assert object_get_func(val, "key1") == "value1"
    assert object_get_func(val, "key3") is None

    # 2. with default
    assert object_get_func(val, "key1", "default") == "value1"
    assert object_get_func(val, "key3", "default") == "default"
