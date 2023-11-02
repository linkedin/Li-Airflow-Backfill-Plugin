from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState

import linkedin.airflow.backfill.dag_operations.dag_deletion as de

import os


def test_delete_backfill_dag(mocker):

    backfill_dag_folder = '/tmp'
    backfill_dag_id = 'test_dag_backfill_000'
    file_path = '/tmp/test_dag_backfill_000.py'
    # create a file for tests
    with open(file_path, 'w') as _:
        pass

    # case: state not deletable
    m_pause_dag = mocker.patch('linkedin.airflow.backfill.dag_operations.dag_deletion.pause_dag')
    m_delete_dag = mocker.patch('linkedin.airflow.backfill.dag_operations.dag_deletion.delete_dag')
    m_model_delete_dag = mocker.patch('linkedin.airflow.backfill.dag_operations.dag_deletion.BackfillModel.delete_dag')
    backfill = BackfillModel(
        state=BackfillState.QUEUED,
    )
    de._delete_backfill_dag(backfill_dag_folder, backfill)
    m_pause_dag.assert_not_called()
    m_delete_dag.assert_not_called()
    m_model_delete_dag.assert_not_called()

    # case: valid
    m_pause_dag = mocker.patch('linkedin.airflow.backfill.dag_operations.dag_deletion.pause_dag')
    m_delete_dag = mocker.patch('linkedin.airflow.backfill.dag_operations.dag_deletion.delete_dag')
    m_model_delete_dag = mocker.patch('linkedin.airflow.backfill.dag_operations.dag_deletion.BackfillModel.delete_dag')
    backfill = BackfillModel(
        state=BackfillState.SUCCESS,
        backfill_dag_id=backfill_dag_id,
    )
    assert os.path.exists(file_path)
    de._delete_backfill_dag(backfill_dag_folder, backfill)
    m_pause_dag.assert_called_once_with(backfill_dag_id)
    m_delete_dag.assert_called_once_with(dag_id=backfill_dag_id)
    m_model_delete_dag.assert_called_once_with(backfill_dag_id)
    assert not os.path.exists(file_path)

    # case: ok to re-run
    m_pause_dag = mocker.patch('linkedin.airflow.backfill.dag_operations.dag_deletion.pause_dag')
    m_delete_dag = mocker.patch('linkedin.airflow.backfill.dag_operations.dag_deletion.delete_dag')
    m_model_delete_dag = mocker.patch('linkedin.airflow.backfill.dag_operations.dag_deletion.BackfillModel.delete_dag')
    backfill = BackfillModel(
        state=BackfillState.SUCCESS,
        backfill_dag_id=backfill_dag_id,
    )
    assert not os.path.exists(file_path)
    de._delete_backfill_dag(backfill_dag_folder, backfill)
    m_pause_dag.assert_called_once_with(backfill_dag_id)
    m_delete_dag.assert_called_once_with(dag_id=backfill_dag_id)
    m_model_delete_dag.assert_called_once_with(backfill_dag_id)
    assert not os.path.exists(file_path)
