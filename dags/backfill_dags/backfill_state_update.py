# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import timedelta
import pendulum

# place within shared dags folder
# quickly update backfill state for common use cases

with DAG(
    "backfill_state_update",
    schedule_interval=timedelta(minutes=6),
    is_paused_upon_creation=False,
    max_active_runs=1,
    start_date=pendulum.datetime(2022, 7, 1, tz="UTC"),
    catchup=False,
) as dag:

    def process():
        from linkedin.airflow.backfill.dag_operations.state_update import update_backfill_states
        from linkedin.airflow.backfill.utils.backfill_state import BackfillState
        update_backfill_states(BackfillState.get_updating_states())

    process_task = PythonOperator(
        task_id='update_state',
        python_callable=process
    )
