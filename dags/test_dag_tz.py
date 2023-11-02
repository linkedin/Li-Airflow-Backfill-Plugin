# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

import pendulum

with DAG(
        'test_dag_tz',
        schedule_interval='@daily',
        start_date=pendulum.datetime(2022, 8, 1, tz="America/Los_Angeles"),
        catchup=False,
        tags=['test'],
        params={"key1": "value1"},
) as dag:

    run_this_last = DummyOperator(
        task_id='run_this_last',
    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id='run_for_2_minutes',
        bash_command='sleep 120',
    )

    run_this >> run_this_last
