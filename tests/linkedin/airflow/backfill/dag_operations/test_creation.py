from tests.linkedin.airflow.backfill.testutils import get_test_dag
import linkedin.airflow.backfill.dag_operations.creation_util as cu

import pendulum
import base64


def test_apply_backfill_dag():
    test_dag = get_test_dag()
    backfill_dag_id = "test_backfill_dag_id"
    start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    end_date = pendulum.datetime(2022, 6, 2, tz='UTC')
    params = '{"test_key_0": "test_value_0"}'
    params_encoded = base64.b64encode(params.encode('utf-8'))

    cu._apply_backfill_dag(
        dag_val=test_dag,
        backfill_dag_id=backfill_dag_id,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        max_active_runs=2,
        dag_params_encoded=params_encoded,
    )
    assert test_dag.dag_id == backfill_dag_id
    assert test_dag.start_date == start_date
    assert test_dag.end_date == end_date
    assert test_dag.catchup is True
    assert test_dag.max_active_runs == 2
    p = test_dag.params.dump()
    assert p["test_key_0"] == "test_value_0"
