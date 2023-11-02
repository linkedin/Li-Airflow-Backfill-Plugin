from airflow.utils import timezone
from datetime import timedelta
from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from tests.linkedin.airflow.backfill.test_models import get_random_backfill
from linkedin.airflow.backfill.utils.request_handling import (
    validate_and_load_backfill,
    validate_and_load_list_request,
    validate_and_load_creation_request,
    validate_and_load_delete_request,
    validate_and_load_cancel_submitted_backfill_request,
    validate_and_load_cancel_backfill_runs_request,
)
from linkedin.airflow.backfill.utils.backfill_util import dict_get_func
from tests.linkedin.airflow.backfill.testutils import TestObj

from unittest.mock import Mock, patch


class FakeDagRun:

    def __init__(self, logical_date):
        self.logical_date = logical_date


class TestDag(TestObj):

    def __init__(self, logical_dates=[], **kwargs):
        super().__init__(**kwargs)
        self._runs = [FakeDagRun(d) for d in logical_dates]

    def iter_dagrun_infos_between(self, start_date, end_date):
        return self._runs


class FakeDagBag:
    def __init__(self, dag=None):
        self._dag = dag

    def get_dag(self, dag_id):
        return self._dag


def test_validate_and_load_backfill():
    column_names = ['backfill_dag_id', 'dag_id']
    o = get_random_backfill()
    return_backfill = o

    # 1. valid id
    mocked_get_backfill_by_id = BackfillModel.get_backfill_by_id = Mock(return_value=return_backfill)
    r, err_msg = validate_and_load_backfill(o.backfill_dag_id, column_names)
    assert r is not None
    assert r.dag_id == o.dag_id
    assert err_msg == ""
    mocked_get_backfill_by_id.assert_called_once_with(column_names, o.backfill_dag_id)
    mocked_get_backfill_by_id.close()

    # 2. non existing
    mocked_get_backfill_by_id = BackfillModel.get_backfill_by_id = Mock(return_value=None)
    r, err_msg = validate_and_load_backfill(o.backfill_dag_id, column_names)
    assert r is None
    assert len(err_msg) > 0
    mocked_get_backfill_by_id.assert_called_once_with(column_names, o.backfill_dag_id)
    mocked_get_backfill_by_id.close()


def test_validate_and_load_list_request():
    curr_dt = timezone.utcnow()
    start_date = curr_dt - timedelta(days=1)
    end_date = curr_dt
    # 1. valid: no params
    data = {}
    r, err_msg = validate_and_load_list_request(data, dict_get_func)
    assert r is not None
    assert r.dag_id is None
    assert r.start_date is None
    assert r.end_date is None
    assert r.include_deleted is False
    assert err_msg == ""

    # 2. valid, with params
    data = {
        "dag_id": "my_dag_id",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "include_deleted": "1",
    }
    r, err_msg = validate_and_load_list_request(data, dict_get_func)
    assert r is not None
    assert r.dag_id == "my_dag_id"
    assert r.start_date == start_date
    assert r.end_date == end_date
    assert r.include_deleted is True
    assert err_msg == ""

    # 3. invalid date str
    data = {
        "start_date": "invalid date",
        "end_date": end_date.isoformat(),
    }
    r, err_msg = validate_and_load_list_request(data, dict_get_func)
    assert r is None
    assert len(err_msg) > 0


def test_validate_and_load_creation_request():
    curr_dt = timezone.utcnow()
    username = "test_user"
    start_date = curr_dt - timedelta(days=1)
    end_date = curr_dt

    # 1. no dag_id
    data = {}
    r, err_msg = validate_and_load_creation_request(data, None, curr_dt, username, dict_get_func)
    assert r is None
    assert err_msg == "dag_id is required"

    # 2. dag not existing
    dag_bag = FakeDagBag()
    data = {
        "dag_id": "my_dag"
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert err_msg == "invalid dag: my_dag"

    # 3. subdag
    dag = TestDag(dag_id='my_dag', is_subdag=True)
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag"
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert err_msg == "subdag is not supported"

    # 4. no start date
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False)
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag"
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert err_msg == "start_date is required"

    # 5. invalid start date
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False)
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": "invalid date",
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert len(err_msg) == 2

    # 6. no end date
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False)
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert err_msg == "end_date is required"

    # 7. invalid end date
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False)
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
        "end_date": "invalid date",
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert len(err_msg) == 2

    # 8. no logical dates in range
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False)
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert err_msg.startswith("no logical date was found between")

    # 9. overlapping
    o1 = get_random_backfill()
    o2 = get_random_backfill()
    o3 = get_random_backfill()
    mocked_get_overlapping_backfills = BackfillModel.get_overlapping_backfills = Mock(return_value=[o1, o2])
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False, logical_dates=[start_date, end_date])
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ignore_overlapping_backfills": False,
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert len(err_msg) == 3
    mocked_get_overlapping_backfills.call_count == 1
    mocked_get_overlapping_backfills.close()

    # 10. more than 2 overlapping
    mocked_get_overlapping_backfills = BackfillModel.get_overlapping_backfills = Mock(return_value=[o1, o2, o3])
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert len(err_msg) == 3
    mocked_get_overlapping_backfills.call_count == 1
    mocked_get_overlapping_backfills.close()

    # 11. invalid dag_params
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False, logical_dates=[start_date, end_date])
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ignore_overlapping_backfills": True,
        "dag_params": '{invalid json',
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert err_msg.startswith("dag_params must be dict")

    # 12. invalid scheduled_date
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False, logical_dates=[start_date, end_date])
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ignore_overlapping_backfills": True,
        "scheduled_date": "invalid date",
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert len(err_msg) == 2

    # 13. invalid max_active_runs
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False, logical_dates=[start_date, end_date])
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ignore_overlapping_backfills": True,
        "max_active_runs": "invalid int",
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert len(err_msg) == 2

    # 14. max_active_runs not in range
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False, logical_dates=[start_date, end_date])
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ignore_overlapping_backfills": True,
        "max_active_runs": "15",
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert err_msg == "max_active_runs must be between 1 to 10"

    # 15. no delete_duration_days
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False, logical_dates=[start_date, end_date])
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ignore_overlapping_backfills": True,
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert err_msg == "delete_duration_days is required"

    # 16. invalid delete_duration_days
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False, logical_dates=[start_date, end_date])
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ignore_overlapping_backfills": True,
        "delete_duration_days": "invalid int",
    }
    r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    assert r is None
    assert len(err_msg) == 2

    # 17. valid
    dag_params = '{"key1": "value1"}'
    scheduled_date = curr_dt + timedelta(days=3)
    dag = TestDag(dag_id='my_dag', is_subdag=False, subdags=False, logical_dates=[start_date, end_date],
                  relative_fileloc="my relative loc", fileloc="my loc")
    dag_bag = FakeDagBag(dag=dag)
    data = {
        "dag_id": "my_dag",
        "description": "my description",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ignore_overlapping_backfills": True,
        "dag_params": dag_params,
        "scheduled_date": scheduled_date.isoformat(),
        "max_active_runs": "2",
        "delete_duration_days": "40",
    }
    dag_code = "my dag code"
    with patch("linkedin.airflow.backfill.utils.request_handling.DagCode.get_code_by_fileloc", Mock(return_value=dag_code)) as mocked_get_code_by_fileloc:
        r, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
        assert r is not None
        assert err_msg == ""
        assert r.backfill_dag_id is not None
        assert r.dag_id == "my_dag"
        assert r.description == "my description"
        assert r.start_date == start_date
        assert r.end_date == end_date
        assert r.dag_params == dag_params
        assert r.scheduled_date == scheduled_date
        assert r.max_active_runs == 2
        assert r.delete_date == curr_dt + timedelta(days=40)
        assert r.state == "submitted"
        assert r.relative_fileloc == "my relative loc"
        assert r.origin_code == "my dag code"
        assert r.submitted_by == username
        assert r.submitted_at == curr_dt
        assert r.updated_at == curr_dt

        mocked_get_code_by_fileloc.assert_called_once_with("my loc")


def test_validate_and_load_delete_request():
    o = get_random_backfill()
    # 1. deleted
    o.deleted = True
    r, err_msg = validate_and_load_delete_request(o)
    assert r is None
    assert err_msg.endswith("was already Deleted")

    # not deletable
    o.deleted = False
    o.state = BackfillState.RUNNING
    r, err_msg = validate_and_load_delete_request(o)
    assert r is None
    assert err_msg.endswith("is not deletable")

    # valid
    o.state = BackfillState.SUCCESS
    o.deleted = False
    r, err_msg = validate_and_load_delete_request(o)
    assert r is not None
    assert r.dag_id == o.dag_id
    assert err_msg == ""


def test_validate_and_load_cancel_submitted_backfill_request():
    o = get_random_backfill()
    curr_dt = timezone.utcnow()
    deadline_date = curr_dt + timedelta(hours=3)

    # 1. deleted
    o.deleted = True
    r, err_msg = validate_and_load_cancel_submitted_backfill_request(o, deadline_date)
    assert r is None
    assert err_msg.endswith("was already Deleted")

    # not in submitted state
    o.deleted = False
    o.state = BackfillState.QUEUED
    r, err_msg = validate_and_load_cancel_submitted_backfill_request(o, deadline_date)
    assert r is None
    assert err_msg.endswith("state must be submitted")

    # too late to cancel
    o.deleted = False
    o.state = BackfillState.SUBMITTED
    o.scheduled_date = deadline_date - timedelta(minutes=3)
    r, err_msg = validate_and_load_cancel_submitted_backfill_request(o, deadline_date)
    assert r is None
    assert err_msg.endswith("cancel it later")

    # valid
    o.deleted = False
    o.state = BackfillState.SUBMITTED
    o.scheduled_date = deadline_date + timedelta(minutes=3)
    r, err_msg = validate_and_load_cancel_submitted_backfill_request(o, deadline_date)
    assert r is not None
    assert r.dag_id == o.dag_id
    assert err_msg == ""


def test_validate_and_load_cancel_backfill_runs_request():
    o = get_random_backfill()
    # 1. deleted
    o.deleted = True
    r, err_msg = validate_and_load_cancel_backfill_runs_request(o)
    assert r is None
    assert err_msg.endswith("was already Deleted")

    # not deletable
    o.deleted = False
    mocked_is_cancelable = BackfillState.is_cancelable = Mock(return_value=False)
    r, err_msg = validate_and_load_cancel_backfill_runs_request(o)
    assert r is None
    assert err_msg.endswith("is not cancellable")
    mocked_is_cancelable.assert_called_once_with(o.state)
    mocked_is_cancelable.close()

    # valid
    mocked_is_cancelable = BackfillState.is_cancelable = Mock(return_value=True)
    o.deleted = False
    r, err_msg = validate_and_load_cancel_backfill_runs_request(o)
    assert r is not None
    assert r.dag_id == o.dag_id
    assert err_msg == ""
    mocked_is_cancelable.assert_called_once_with(o.state)
    mocked_is_cancelable.close()
