# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

import pendulum

with DAG(
        'test_dag_utc',
        schedule_interval='@daily',
        start_date=pendulum.datetime(2022, 8, 1, tz="UTC"),
        catchup=False,
        tags=['test'],
        default_args={
            "params": {
                "key2": "value2_default_args",
                "key3": "value3_default_args",
            },
        },
        params={
            "key1": "value1",
            "key2": "value2",
        },
) as dag:

    run_this_last = DummyOperator(
        task_id='run_this_last',
    )

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id='echo_params',
        bash_command='echo "key1={{params.key1}}, key2={{params.key2}}, key3={{params.key3}}, key4={{params.key4}}"',
        params={
            "key2": "value2_task",
            "key4": "value4_task",
        },
    )

    run_this >> run_this_last
