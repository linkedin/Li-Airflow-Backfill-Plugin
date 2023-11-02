from tests.linkedin.airflow.backfill.test_models import get_random_backfill
from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.utils.request_handling import ListBackfillParams
from linkedin.airflow.backfill.apis.api_impl import (
    list_backfills,
    cancel_backfill,
    get_backfill,
    create_backfill,
    delete_backfill,
)
from airflow.api_connexion.exceptions import NotFound, PermissionDenied

import pendulum

from unittest.mock import Mock, patch
import pytest


def test_list_backfills():
    o1 = get_random_backfill()
    o2 = get_random_backfill()
    o2.deleted = True
    start_date = pendulum.datetime(2023, 1, 1, tz='UTC')
    end_date = pendulum.datetime(2023, 1, 2, tz='UTC')
    o3 = get_random_backfill(start_date=start_date, end_date=end_date)
    return_backfills = [o1, o2, o3]
    permitted_dag_ids = [o1.backfill_dag_id, o2.dag_id, o3.backfill_dag_id]
    data = None     # not in use, mocked

    # 1. no filter, all permitted
    mocked_api_list_backfills = BackfillModel.list_backfills = Mock(return_value=return_backfills)
    list_params = ListBackfillParams()
    with patch("linkedin.airflow.backfill.apis.api_impl.validate_and_load_list_request", Mock(return_value=(list_params, ""))) as mocked_load:
        r = list_backfills(permitted_dag_ids, data)
        assert len(r) == 2
        assert r[0]["backfill_dag_id"] == o1.backfill_dag_id
        assert r[1]["backfill_dag_id"] == o3.backfill_dag_id
        assert mocked_api_list_backfills.call_count == 1
        assert mocked_load.call_count == 1
    mocked_api_list_backfills.close()

    # 2. filter by permitted
    mocked_api_list_backfills = BackfillModel.list_backfills = Mock(return_value=return_backfills)
    list_params = ListBackfillParams()
    with patch("linkedin.airflow.backfill.apis.api_impl.validate_and_load_list_request", Mock(return_value=(list_params, ""))) as mocked_load:
        r = list_backfills([o2.dag_id, o3.backfill_dag_id], data)
        assert len(r) == 1
        assert r[0]["backfill_dag_id"] == o3.backfill_dag_id
        assert mocked_api_list_backfills.call_count == 1
        assert mocked_load.call_count == 1
    mocked_api_list_backfills.close()

    # filter by include_deleted
    mocked_api_list_backfills = BackfillModel.list_backfills = Mock(return_value=return_backfills)
    list_params = ListBackfillParams(include_deleted=True)
    with patch("linkedin.airflow.backfill.apis.api_impl.validate_and_load_list_request", Mock(return_value=(list_params, ""))) as mocked_load:
        r = list_backfills(permitted_dag_ids, data)
        assert len(r) == 3
        assert mocked_api_list_backfills.call_count == 1
        assert mocked_load.call_count == 1
    mocked_api_list_backfills.close()

    # filter by dates
    mocked_api_list_backfills = BackfillModel.list_backfills = Mock(return_value=return_backfills)
    list_params = ListBackfillParams(start_date=start_date, end_date=start_date)
    with patch("linkedin.airflow.backfill.apis.api_impl.validate_and_load_list_request", Mock(return_value=(list_params, ""))) as mocked_load:
        r = list_backfills(permitted_dag_ids, data)
        assert len(r) == 1
        assert r[0]["backfill_dag_id"] == o3.backfill_dag_id
        assert mocked_api_list_backfills.call_count == 1
        assert mocked_load.call_count == 1
    mocked_api_list_backfills.close()


def test_cancel_backfill():
    o1 = get_random_backfill()
    o1.state = BackfillState.SUBMITTED
    o2 = get_random_backfill()
    o2.state = BackfillState.RUNNING
    dag_bag = None  # not in use, mocked

    # 1. submitted
    mocked_delete_submitted_backfills = BackfillModel.delete_submitted_backfills = Mock()
    with patch("linkedin.airflow.backfill.apis.api_impl._get_backfill_by_id", Mock(return_value=o1)) as mocked_get_backfill_by_id:
        with patch(
                "linkedin.airflow.backfill.apis.api_impl.validate_and_load_cancel_submitted_backfill_request",
                Mock(return_value=(o1, ""))
        ) as mocked_validate_and_load_cancel_submitted_backfill_request:
            r = cancel_backfill(o1.backfill_dag_id, dag_bag)
            assert r["backfill_dag_id"] == o1.backfill_dag_id
            assert mocked_get_backfill_by_id.call_count == 2
            assert mocked_validate_and_load_cancel_submitted_backfill_request.call_count == 1
            assert mocked_delete_submitted_backfills.call_count == 1
    mocked_delete_submitted_backfills.close()

    # 2. dag runs
    mocked_get_active_backfills = BackfillModel.get_active_backfills = Mock(return_value=[o2])
    with patch("linkedin.airflow.backfill.apis.api_impl._get_backfill_by_id", Mock(return_value=o2)) as mocked_get_backfill_by_id:
        with patch(
                "linkedin.airflow.backfill.apis.api_impl.validate_and_load_cancel_backfill_runs_request",
                Mock(return_value=(o2, ""))
        ) as mocked_validate_and_load_cancel_backfill_runs_request:
            with patch(
                    "linkedin.airflow.backfill.utils.operations.cancel_backfill_dag_runs",
                    Mock(return_value=(1, ""))
            ) as mocked_cancel_backfill_dag_runs:
                with patch(
                        "linkedin.airflow.backfill.utils.operations.update_states",
                        Mock(return_value=(1, ""))
                ) as mocked_update_states:
                    r = cancel_backfill(o2.backfill_dag_id, dag_bag)
                    assert r["backfill_dag_id"] == o2.backfill_dag_id
                    assert mocked_get_backfill_by_id.call_count == 2
                    assert mocked_validate_and_load_cancel_backfill_runs_request.call_count == 1
                    assert mocked_cancel_backfill_dag_runs.call_count == 1
                    assert mocked_update_states.call_count == 1
                    assert mocked_get_active_backfills.call_count == 1
    mocked_get_active_backfills.close()


def test_get_backfill():
    o = get_random_backfill()

    # 1. id does not exist
    with patch(
            "linkedin.airflow.backfill.apis.api_impl.validate_and_load_backfill",
            Mock(return_value=(None, "not found error message"))
    ) as mocked_validate_and_load_backfill:
        with pytest.raises(NotFound) as exc:
            get_backfill("nonexisiting-id")
            assert exc.title == "Backfill not found"
            assert exc.detail == "not found error message"
            assert mocked_validate_and_load_backfill.call_count == 1

    # 2. valid id
    with patch(
            "linkedin.airflow.backfill.apis.api_impl.validate_and_load_backfill",
            Mock(return_value=(o, ""))
    ) as mocked_validate_and_load_backfill:
        r = get_backfill(o.backfill_dag_id)
        assert r["backfill_dag_id"] == o.backfill_dag_id
        assert mocked_validate_and_load_backfill.call_count == 1


def test_delete_backfill():
    o = get_random_backfill()

    # 1. mark delete
    mocked_update_delete_date = BackfillModel.update_delete_date = Mock()
    with patch("linkedin.airflow.backfill.apis.api_impl._get_backfill_by_id", Mock(return_value=o)) as mocked_get_backfill_by_id:
        with patch(
                "linkedin.airflow.backfill.apis.api_impl.validate_and_load_delete_request",
                Mock(return_value=(o, ""))
        ) as mocked_validate_and_load_delete_request:
            r = delete_backfill(o.backfill_dag_id)
            assert r["backfill_dag_id"] == o.backfill_dag_id
            assert mocked_get_backfill_by_id.call_count == 2
            assert mocked_validate_and_load_delete_request.call_count == 1
            assert mocked_update_delete_date.call_count == 1
    mocked_update_delete_date.close()


def test_create_backfill():
    o1 = get_random_backfill()
    o2 = get_random_backfill()
    dag_bag = None  # not in use, mocked
    username = "test-user"

    # 1. not in permission list
    with patch(
            "linkedin.airflow.backfill.apis.api_impl.validate_and_load_creation_request",
            Mock(return_value=(o1, ""))
    ) as mocked_validate_and_load_creation_request:
        with pytest.raises(PermissionDenied):
            permitted_dag_ids = [o2.dag_id]
            r = create_backfill(permitted_dag_ids, o1, dag_bag, username)
            assert mocked_validate_and_load_creation_request.call_count == 1

    # 2. create backfill
    mocked_add_backfill = BackfillModel.add_backfill = Mock()
    with patch("linkedin.airflow.backfill.apis.api_impl._get_backfill_by_id", Mock(return_value=o1)) as mocked_get_backfill_by_id:
        with patch(
                "linkedin.airflow.backfill.apis.api_impl.validate_and_load_creation_request",
                Mock(return_value=(o1, ""))
        ) as mocked_validate_and_load_creation_request:
            permitted_dag_ids = [o1.dag_id]
            r = create_backfill(permitted_dag_ids, o1, dag_bag, username)
            assert r["backfill_dag_id"] == o1.backfill_dag_id
            assert mocked_get_backfill_by_id.call_count == 1
            assert mocked_validate_and_load_creation_request.call_count == 1
            mocked_add_backfill.call_count == 1
    mocked_add_backfill.close()
