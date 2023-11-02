# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

def _apply_backfill_dag(
        dag_val,
        backfill_dag_id,
        start_date,
        end_date,
        max_active_runs,
        dag_params_encoded,
):
    """
    modify dag attributes to change a dag to its backfill dag
    """
    from airflow.utils import timezone
    import base64
    import json
    from airflow.models.param import ParamsDict

    start_date_dt = timezone.parse(start_date)
    end_date_dt = timezone.parse(end_date)

    dag_val.dag_id = backfill_dag_id
    dag_val.safe_dag_id = backfill_dag_id.replace('.', '__dot__')
    dag_val.start_date = start_date_dt
    dag_val.end_date = end_date_dt
    dag_val.catchup = True
    dag_val.max_active_runs = max_active_runs
    dag_val.is_paused_upon_creation = False

    # override params
    additional_params = json.loads(base64.b64decode(dag_params_encoded).decode('utf-8')) if dag_params_encoded else None
    dag_params = dag_val.params.dump() if dag_val.params else {}
    if additional_params:
        dag_params.update(additional_params)
        dag_val.params = ParamsDict(dag_params)

    """Sets up all tasks within the backfill Dag to ensure it runs properly."""
    for task in dag_val.tasks:
        # Airflow looks at latest execution_date and adds the schedule_interval to determine the next interval date
        task.start_date = start_date_dt
        task.end_date = end_date_dt
        # task params
        if additional_params:
            task_params = task.params.dump()
            task_params.update(additional_params)
            task.params = ParamsDict(task_params)


def apply_backfill(
        gl,
        dag_id,
        backfill_dag_id,
        start_date,
        end_date,
        max_active_runs,
        dag_params_encoded,
):
    """
    modify dags attributes to their backfill dags
    add backfill dags to globals for Dag Parser
    """
    from airflow import DAG
    from airflow.models.dag import DagContext

    found_dag = None
    found_dag_variable_name = None

    # check globals
    dag_variable_names = []
    for var_name, dag_val in gl.items():
        if not isinstance(dag_val, DAG):
            continue
        dag_variable_names.append(var_name)
        if dag_val.dag_id != dag_id:
            continue
        found_dag_variable_name = var_name
        found_dag = dag_val

    # check dag context
    for dag_val, _ in DagContext.autoregistered_dags:
        if dag_val.dag_id == dag_id:
            found_dag = dag_val

    # apply options to get backfill dag
    _apply_backfill_dag(
        dag_val=found_dag,
        backfill_dag_id=backfill_dag_id,
        start_date=start_date,
        end_date=end_date,
        max_active_runs=max_active_runs,
        dag_params_encoded=dag_params_encoded,
    )

    # clear non backfilling dags
    for var_name in dag_variable_names:
        del gl[var_name]
    DagContext.autoregistered_dags.clear()

    var_name = found_dag_variable_name if found_dag_variable_name else backfill_dag_id
    gl[var_name] = found_dag
