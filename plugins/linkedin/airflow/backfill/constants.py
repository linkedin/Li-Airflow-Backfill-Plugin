# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import os


AIRFLOW_FOLDER = os.getenv("AIRFLOW_HOME") or "/opt/airflow"

# backfill origin code snapshot date format
BACKFILL_ORIGIN_CODE_SNAPSHOT_DATE_FORMAT = "%Y-%m-%d"

# loopback days for backfill overlapping check
BACKFILL_OVERLAPPING_LOOPBACK_DAYS = 7

# max overlapping dags to show
BACKFILL_MAX_OVERLAPPING_DAGS_TO_SHOW = 2

# max active dag runs
BACKFILL_MAX_ACTIVE_DAG_RUNS = 10

# get submitted dags limit
BACKFILL_GET_SUBMITTED_DAGS_LIMIT = 30

# tolerance minutes before scheduled date
BACKFILL_SCHEDULED_DATE_TOLERANCE_MINS = 3

# backfill dag creation wait min
BACKFILL_DAG_CREATION_WAIT_MINS = 10


class FieldKeyConstants:
    """
    keep field key constants
    """
    BACKFILL_DAG_ID = "backfill_dag_id"
    DAG_ID = "dag_id"
    DESCRIPTION = "description"
    START_DATE = "start_date"
    END_DATE = "end_date"
    LOGICAL_DATES = "logical_dates"
    DAG_PARAMS = "dag_params"
    SCHEDULED_DATE = "scheduled_date"
    MAX_ACTIVE_RUNS = "max_active_runs"
    DELETE_DATE = "delete_date"
    STATE = "state"
    RELATIVE_FILELOC = "relative_fileloc"
    ORIGIN_CODE = "origin_code"
    SUBMITTED_BY = "submitted_by"
    SUBMITTED_AT = "submitted_at"
    UPDATED_AT = "updated_at"
    DELETED = "deleted"
    RUN_START_DATE = "run_start_date"
    RUN_END_DATE = "run_end_date"
    CODE = "code"

    IGNORE_OVERLAPPING_BACKFILLS = "ignore_overlapping_backfills"
    DELETE_DURATION_DAYS = "delete_duration_days"
    INCLUDE_DELETED = "include_deleted"
    SCHEDULED_AT = "scheduled_at"
