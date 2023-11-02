# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import timedelta
import pendulum

# place within shared dags folder
# create backfills

with DAG(
    "backfill_creation",
    schedule_interval=timedelta(minutes=3),
    is_paused_upon_creation=False,
    max_active_runs=1,
    start_date=pendulum.datetime(2022, 7, 1, tz="UTC"),
    catchup=False,
) as dag:

    def process():
        from linkedin.airflow.backfill.dag_operations.creation import process_submitted_backfills
        process_submitted_backfills()

    process_task = PythonOperator(
        task_id='Dag_Creation',
        python_callable=process
    )
