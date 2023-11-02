from airflow.utils import timezone
from airflow.utils.state import State

import linkedin.airflow.backfill.utils.operations as ops
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.models.backfill import BackfillModel

from tests.linkedin.airflow.backfill.testutils import TestObj

import pendulum
from datetime import timedelta


class TestDagRun(TestObj):
    def get_state(self):
        return getattr(self, 'state')


def test_parse_dag_runs():

    logical_dates_set = {
        pendulum.datetime(2022, 6, 1, tz='UTC'),
        pendulum.datetime(2022, 6, 2, tz='UTC'),
        pendulum.datetime(2022, 6, 3, tz='UTC'),
    }

    curr_dt = timezone.utcnow()
    run_start_date = curr_dt - timedelta(hours=3)
    non_logical_date_runs = [
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, 10, 0, 5, tz='UTC'),
            state=State.FAILED,
            start_date=run_start_date,
            end_date=run_start_date + timedelta(minutes=20),
        ),
    ]

    def test_case(dag_runs, expected_state, expected_min_date, expected_max_date):
        state, min_date, max_date = ops._parse_dag_runs(logical_dates_set, dag_runs)
        assert state == expected_state
        assert min_date == expected_min_date
        assert max_date == expected_max_date

        # non logical date runs should be ignored
        dag_runs.extend(non_logical_date_runs)
        state, min_date, max_date = ops._parse_dag_runs(logical_dates_set, dag_runs)
        assert state == expected_state
        assert min_date == expected_min_date
        assert max_date == expected_max_date

    # case: no runs
    dag_runs = []
    state, min_date, max_date = ops._parse_dag_runs(logical_dates_set, dag_runs)
    assert state == BackfillState.RUNNING
    assert min_date is None
    assert max_date is None

    # case: only runs are in unfinished state
    dag_runs = [
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.RUNNING,
            start_date=run_start_date,
            end_date=None,
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 2, tz='UTC'),
            state=State.QUEUED,
            start_date=None,
            end_date=None,
        ),
    ]
    test_case(dag_runs, BackfillState.RUNNING, run_start_date, None)

    # case: only runs are in unfinished and success state
    dag_runs = [
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date,
            end_date=run_start_date + timedelta(minutes=3),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 2, tz='UTC'),
            state=State.RUNNING,
            start_date=run_start_date + timedelta(minutes=3),
            end_date=None,
        ),
    ]
    test_case(dag_runs, BackfillState.RUNNING, run_start_date, None)

    # case: partial success
    dag_runs = [
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date,
            end_date=run_start_date + timedelta(minutes=3),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 2, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=3),
            end_date=run_start_date + timedelta(minutes=5),
        ),
    ]
    test_case(dag_runs, BackfillState.RUNNING, run_start_date, None)

    # case: success
    dag_runs = [
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date,
            end_date=run_start_date + timedelta(minutes=3),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 2, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=3),
            end_date=run_start_date + timedelta(minutes=5),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 3, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=6),
            end_date=run_start_date + timedelta(minutes=10),
        ),
    ]
    test_case(dag_runs, BackfillState.SUCCESS, run_start_date, run_start_date + timedelta(minutes=10))

    # case: failed but unfinished
    dag_runs = [
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.FAILED,
            start_date=run_start_date,
            end_date=run_start_date + timedelta(minutes=3),
        ),
    ]
    test_case(dag_runs, BackfillState.FAILED, run_start_date, None)

    # case: failed
    dag_runs = [
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.FAILED,
            start_date=run_start_date,
            end_date=run_start_date + timedelta(minutes=3),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 2, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=3),
            end_date=run_start_date + timedelta(minutes=5),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 3, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=6),
            end_date=run_start_date + timedelta(minutes=10),
        ),
    ]
    test_case(dag_runs, BackfillState.FAILED, run_start_date, run_start_date + timedelta(minutes=10))

    # case: multiple runs in the same logical date
    dag_runs = [
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.FAILED,
            start_date=run_start_date,
            end_date=run_start_date + timedelta(minutes=3),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.RUNNING,
            start_date=run_start_date + timedelta(minutes=16),
            end_date=None,
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 2, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=3),
            end_date=run_start_date + timedelta(minutes=5),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 3, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=6),
            end_date=run_start_date + timedelta(minutes=10),
        ),
    ]
    test_case(dag_runs, BackfillState.RUNNING, run_start_date, None)
    dag_runs = [
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.FAILED,
            start_date=run_start_date,
            end_date=run_start_date + timedelta(minutes=3),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 1, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=16),
            end_date=run_start_date + timedelta(minutes=20),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 2, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=3),
            end_date=run_start_date + timedelta(minutes=5),
        ),
        TestDagRun(
            logical_date=pendulum.datetime(2022, 6, 3, tz='UTC'),
            state=State.SUCCESS,
            start_date=run_start_date + timedelta(minutes=6),
            end_date=run_start_date + timedelta(minutes=10),
        ),
    ]
    test_case(dag_runs, BackfillState.SUCCESS, run_start_date, run_start_date + timedelta(minutes=20))


def test_update_state(mocker):
    # mock db update
    m = mocker.patch("linkedin.airflow.backfill.utils.operations.BackfillModel.update_run_state")
    mocker.patch('linkedin.airflow.backfill.utils.operations.parse_logical_dates_str', return_value=None)

    curr_dt = timezone.utcnow()
    min_date = curr_dt - timedelta(hours=3)
    max_date = min_date + timedelta(hours=1)
    dag_runs = None

    # case: not updatable
    backfill = BackfillModel(
        state=BackfillState.SUBMITTED,
    )
    ops._update_state(backfill, False, dag_runs)
    m.assert_not_called()

    # case: update to running
    m = mocker.patch("linkedin.airflow.backfill.utils.operations.BackfillModel.update_run_state")
    mocker.patch("linkedin.airflow.backfill.utils.operations.DagModel.get_current", return_value=1)
    mocker.patch('linkedin.airflow.backfill.utils.operations._parse_dag_runs', return_value=(BackfillState.RUNNING, min_date, None))
    backfill = BackfillModel(
        backfill_dag_id='my_backfill_dag_id',
        state=BackfillState.QUEUED,
        updated_at=curr_dt,
    )
    ops._update_state(backfill, False, dag_runs)
    m.assert_called_once_with(backfill.backfill_dag_id, backfill.updated_at, BackfillState.RUNNING, min_date, None)

    # case: update to pause
    m = mocker.patch("linkedin.airflow.backfill.utils.operations.BackfillModel.update_run_state")
    mocker.patch("linkedin.airflow.backfill.utils.operations.DagModel.get_current", return_value=1)
    mocker.patch('linkedin.airflow.backfill.utils.operations._parse_dag_runs', return_value=(BackfillState.RUNNING, min_date, None))
    backfill = BackfillModel(
        backfill_dag_id='my_backfill_dag_id',
        state=BackfillState.QUEUED,
        updated_at=curr_dt,
    )
    ops._update_state(backfill, True, dag_runs)
    m.assert_called_once_with(backfill.backfill_dag_id, backfill.updated_at, BackfillState.PAUSED, min_date, None)

    # case: update to finished state
    m = mocker.patch("linkedin.airflow.backfill.utils.operations.BackfillModel.update_run_state")
    mocker.patch("linkedin.airflow.backfill.utils.operations.DagModel.get_current", return_value=1)
    mocker.patch('linkedin.airflow.backfill.utils.operations._parse_dag_runs', return_value=(BackfillState.SUCCESS, min_date, max_date))
    backfill = BackfillModel(
        backfill_dag_id='my_backfill_dag_id',
        state=BackfillState.QUEUED,
        updated_at=curr_dt,
    )
    ops._update_state(backfill, False, dag_runs)
    m.assert_called_once_with(backfill.backfill_dag_id, backfill.updated_at, BackfillState.SUCCESS, min_date, max_date)

    # case: update to finished state with pause
    m = mocker.patch("linkedin.airflow.backfill.utils.operations.BackfillModel.update_run_state")
    mocker.patch("linkedin.airflow.backfill.utils.operations.DagModel.get_current", return_value=1)
    mocker.patch('linkedin.airflow.backfill.utils.operations._parse_dag_runs', return_value=(BackfillState.SUCCESS, min_date, max_date))
    backfill = BackfillModel(
        backfill_dag_id='my_backfill_dag_id',
        state=BackfillState.QUEUED,
        updated_at=curr_dt,
    )
    ops._update_state(backfill, True, dag_runs)
    m.assert_called_once_with(backfill.backfill_dag_id, backfill.updated_at, BackfillState.SUCCESS, min_date, max_date)

    # case: backfill dag not exist, skip
    m = mocker.patch("linkedin.airflow.backfill.utils.operations.BackfillModel.update_run_state")
    mocker.patch("linkedin.airflow.backfill.utils.operations.DagModel.get_current", return_value=None)
    backfill = BackfillModel(
        backfill_dag_id='my_backfill_dag_id',
        state=BackfillState.QUEUED,
        updated_at=curr_dt,
    )
    ops._update_state(backfill, False, dag_runs)
    m.assert_not_called()

    # case: backfill dag not exist, set fail
    m = mocker.patch("linkedin.airflow.backfill.utils.operations.BackfillModel.update_run_state")
    mocker.patch("linkedin.airflow.backfill.utils.operations.DagModel.get_current", return_value=None)
    backfill = BackfillModel(
        backfill_dag_id='my_backfill_dag_id',
        state=BackfillState.QUEUED,
        updated_at=curr_dt - timedelta(hours=2),
    )
    ops._update_state(backfill, False, dag_runs)
    m.assert_called_once_with(backfill.backfill_dag_id, backfill.updated_at, BackfillState.FAILED, None, None)

    # case: no backfill, no _get_dag_runs called
    m = mocker.patch("linkedin.airflow.backfill.utils.operations._get_dag_runs")
    ops.update_states([])
    m.assert_not_called()
