# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow.utils import timezone
from airflow.models.dagcode import DagCode

from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.utils.backfill_util import (
    try_json_to_dict,
    object_get_func,
    parse_backfill_deleted,
)
from linkedin.airflow.backfill.constants import (
    BACKFILL_OVERLAPPING_LOOPBACK_DAYS,
    BACKFILL_MAX_OVERLAPPING_DAGS_TO_SHOW,
    BACKFILL_MAX_ACTIVE_DAG_RUNS,
    FieldKeyConstants,
)

from linkedin.airflow.backfill.utils.backfill_store import backfill_store

from datetime import datetime, timedelta
import json


class ListBackfillParams:

    def __init__(self, dag_id=None, start_date=None, end_date=None, include_deleted=False):
        self.dag_id = dag_id
        self.start_date = start_date
        self.end_date = end_date
        self.include_deleted = include_deleted


def validate_and_load_backfill(backfill_dag_id, columns):
    """
    validate if backfill exists, return backfill
    """
    backfill = BackfillModel.get_backfill_by_id(columns, backfill_dag_id)
    return backfill, "" if backfill else f"The backfill with backfill_dag_id: `{backfill_dag_id}` was not found"


def validate_and_load_list_request(data, get_func=object_get_func):
    """
    list backfill request, return filter params, raise exceptions if errors
    """
    try:
        # start_date, end_date
        start_date = get_func(data, FieldKeyConstants.START_DATE)
        if start_date and not isinstance(start_date, datetime):
            start_date = timezone.parse(start_date)
        end_date = get_func(data, FieldKeyConstants.END_DATE)
        if end_date and not isinstance(end_date, datetime):
            end_date = timezone.parse(end_date)

        # include_deleted
        include_deleted = get_func(data, FieldKeyConstants.INCLUDE_DELETED, False)
        if not isinstance(include_deleted, bool):
            include_deleted = include_deleted == "1" or include_deleted.lower() == "true"

        return ListBackfillParams(
            dag_id=get_func(data, FieldKeyConstants.DAG_ID),
            start_date=start_date,
            end_date=end_date,
            include_deleted=include_deleted,
        ), ""

    except Exception as ex:
        import traceback
        return None, [f'unknown error: {ex}', traceback.format_exc()]


def validate_and_load_creation_request(data, dag_bag, curr_dt, username, get_func=object_get_func):
    """
    create backfill from creation request, return backfill, messages if errors
    """
    try:
        # dag
        dag_id = get_func(data, FieldKeyConstants.DAG_ID, '')
        if not dag_id:
            return None, 'dag_id is required'
        dag = dag_bag.get_dag(dag_id)
        if not dag:
            return None, f'invalid dag: {dag_id}'
        # subdag is not supported yet
        if dag.is_subdag or dag.subdags:
            return None, 'subdag is not supported'

        # available dates in range
        start_date = get_func(data, FieldKeyConstants.START_DATE)
        if not start_date:
            return None, 'start_date is required'
        if not isinstance(start_date, datetime):
            start_date = timezone.parse(start_date)
        end_date = get_func(data, FieldKeyConstants.END_DATE)
        if not end_date:
            return None, 'end_date is required'
        if not isinstance(end_date, datetime):
            end_date = timezone.parse(end_date)

        # logical_dates
        runs = dag.iter_dagrun_infos_between(start_date, end_date)
        if not any(True for _ in runs):
            return None, f'no logical date was found between start_date({start_date}) and end_date({end_date})'
        logical_dates = [run.logical_date.isoformat() for run in runs]

        # overlapping backfills
        ignore_overlapping_backfills = get_func(data, FieldKeyConstants.IGNORE_OVERLAPPING_BACKFILLS, False)
        if not isinstance(ignore_overlapping_backfills, bool):
            ignore_overlapping_backfills = str(ignore_overlapping_backfills).lower() in ["true", "1"]
        if not ignore_overlapping_backfills:
            loopback_date = curr_dt - timedelta(days=BACKFILL_OVERLAPPING_LOOPBACK_DAYS)
            overlapping_dags = BackfillModel.get_overlapping_backfills(dag_id, start_date, end_date, loopback_date)
            if overlapping_dags:
                count = len(overlapping_dags)
                # include samples to message
                msg_dags = overlapping_dags if count <= BACKFILL_MAX_OVERLAPPING_DAGS_TO_SHOW else overlapping_dags[0: BACKFILL_MAX_OVERLAPPING_DAGS_TO_SHOW]
                msgs = []
                msgs.append(f'found {count} overlapping backfill(s), please verify or set "ignore_overlapping_backfills" to True to continue. '
                            + f'showing {len(msg_dags)} sample(s)')
                for dag in msg_dags:
                    msgs.append(f'{dag.backfill_dag_id}: state: {dag.state}, date range: [{dag.start_date}, {dag.end_date}]')
                return None, msgs

        # dag_params
        dag_params = get_func(data, FieldKeyConstants.DAG_PARAMS, '{}')
        _, err_msg = try_json_to_dict(dag_params)
        if err_msg:
            return None, f'dag_params must be dict: {err_msg}'

        # scheduled_date
        scheduled_date = get_func(data, FieldKeyConstants.SCHEDULED_DATE, curr_dt)
        if not isinstance(scheduled_date, datetime):
            scheduled_date = timezone.parse(scheduled_date)

        # max_active_runs
        max_active_runs = get_func(data, FieldKeyConstants.MAX_ACTIVE_RUNS, 1)
        if not isinstance(max_active_runs, int):
            max_active_runs = int(max_active_runs)
        if max_active_runs <= 0 or max_active_runs > BACKFILL_MAX_ACTIVE_DAG_RUNS:
            return None, f'max_active_runs must be between 1 to {BACKFILL_MAX_ACTIVE_DAG_RUNS}'

        # delete_duration_days
        delete_duration_days = get_func(data, FieldKeyConstants.DELETE_DURATION_DAYS)
        if not delete_duration_days:
            return None, 'delete_duration_days is required'
        if not isinstance(delete_duration_days, int):
            delete_duration_days = int(delete_duration_days)

        # origin_code
        origin_code = DagCode.get_code_by_fileloc(dag.fileloc)

        return BackfillModel(
            backfill_dag_id=backfill_store.generate_backfill_dag_id(dag_id, curr_dt),
            dag_id=dag_id,
            description=get_func(data, FieldKeyConstants.DESCRIPTION, ''),
            start_date=start_date,
            end_date=end_date,
            logical_dates=json.dumps(logical_dates),
            dag_params=dag_params,
            scheduled_date=scheduled_date,
            max_active_runs=max_active_runs,
            delete_date=curr_dt + timedelta(days=delete_duration_days),
            state=BackfillState.SUBMITTED,
            relative_fileloc=str(dag.relative_fileloc),
            origin_code=origin_code,
            submitted_by=username,
            submitted_at=curr_dt,
            updated_at=curr_dt,
        ), ""
    except Exception as ex:
        import traceback
        return None, [f'unknown error: {ex}', traceback.format_exc()]


def validate_and_load_delete_request(backfill):
    """
    make sure backfill can be deleted, return backfill and error message
    """
    deleted_val = parse_backfill_deleted(backfill)
    if deleted_val:
        return None, f"backfill {backfill.backfill_dag_id} was already {deleted_val}"
    if not BackfillState.is_deletable(backfill.state):
        return None, f"backfill {backfill.backfill_dag_id} state {backfill.state} is not deletable"
    return backfill, ""


def validate_and_load_cancel_submitted_backfill_request(backfill, deadline_date):
    """
    make sure backfill can be cancelled, return backfill and error message
    """
    deleted_val = parse_backfill_deleted(backfill)
    if deleted_val:
        return None, f"backfill {backfill.backfill_dag_id} was already {deleted_val}"
    if backfill.state != BackfillState.SUBMITTED:
        return None, f"backfill {backfill.backfill_dag_id} state must be {BackfillState.SUBMITTED}"

    if backfill.scheduled_date <= deadline_date:
        return None, f"backfill {backfill.backfill_dag_id} is scheduling, cancel it later"
    return backfill, ""


def validate_and_load_cancel_backfill_runs_request(backfill):
    """
    make sure backfill can be cancelled, return backfill and error message
    """
    deleted_val = parse_backfill_deleted(backfill)
    if deleted_val:
        return None, f"backfill {backfill.backfill_dag_id} was already {deleted_val}"
    if not BackfillState.is_cancelable(backfill.state):
        return None, f"backfill {backfill.backfill_dag_id} state {backfill.state} is not cancellable"
    return backfill, ""
