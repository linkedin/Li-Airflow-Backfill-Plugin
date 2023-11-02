# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from datetime import timedelta
from airflow.utils import timezone
from airflow.api_connexion.exceptions import NotFound, BadRequest, PermissionDenied, Unknown
from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.utils.request_handling import (
    validate_and_load_backfill,
    validate_and_load_list_request,
    validate_and_load_creation_request,
    validate_and_load_delete_request,
    validate_and_load_cancel_submitted_backfill_request,
    validate_and_load_cancel_backfill_runs_request
)
from linkedin.airflow.backfill.utils.backfill_util import (
    parse_backfill_deleted,
    is_backfill_permitted,
    is_any_logical_date_in_range,
    parse_logical_dates_str,
    dict_get_func,
    flat_error_messages,
)
from linkedin.airflow.backfill.constants import (
    BACKFILL_SCHEDULED_DATE_TOLERANCE_MINS,
    FieldKeyConstants,
)


import json
import logging


BACKFILL_COLUMN_NAMES = [
    FieldKeyConstants.BACKFILL_DAG_ID,
    FieldKeyConstants.DAG_ID,
    FieldKeyConstants.STATE,
    FieldKeyConstants.DESCRIPTION,
    FieldKeyConstants.START_DATE,
    FieldKeyConstants.END_DATE,
    FieldKeyConstants.LOGICAL_DATES,
    FieldKeyConstants.DAG_PARAMS,
    FieldKeyConstants.MAX_ACTIVE_RUNS,
    FieldKeyConstants.SCHEDULED_DATE,
    FieldKeyConstants.DELETE_DATE,
    FieldKeyConstants.SUBMITTED_AT,
    FieldKeyConstants.SUBMITTED_BY,
    FieldKeyConstants.UPDATED_AT,
    FieldKeyConstants.DELETED,
    FieldKeyConstants.RUN_START_DATE,
    FieldKeyConstants.RUN_END_DATE,
]


def _backfill_to_response(backfill, curr_dt):
    """
    convert backfill model to response format (dict)
    """
    result = {
        FieldKeyConstants.BACKFILL_DAG_ID: backfill.backfill_dag_id,
        FieldKeyConstants.DESCRIPTION: backfill.description,
        FieldKeyConstants.DAG_ID: backfill.dag_id,
        FieldKeyConstants.STATE: backfill.state.value,
        FieldKeyConstants.START_DATE: backfill.start_date.isoformat(),
        FieldKeyConstants.END_DATE: backfill.end_date.isoformat(),
        FieldKeyConstants.MAX_ACTIVE_RUNS: backfill.max_active_runs,
        FieldKeyConstants.SUBMITTED_BY: backfill.submitted_by,
        FieldKeyConstants.SUBMITTED_AT: backfill.submitted_at.isoformat(),
        FieldKeyConstants.UPDATED_AT: backfill.updated_at.isoformat(),
        # either mark deletion or deleted
        FieldKeyConstants.DELETED: parse_backfill_deleted(backfill, curr_dt),
    }
    if backfill.logical_dates:
        result[FieldKeyConstants.LOGICAL_DATES] = [d.isoformat() for d in parse_logical_dates_str(backfill.logical_dates)]
    if backfill.scheduled_date:
        result[FieldKeyConstants.SCHEDULED_AT] = backfill.scheduled_date.isoformat()
    if backfill.dag_params:
        result[FieldKeyConstants.DAG_PARAMS] = json.loads(backfill.dag_params)
    if backfill.run_start_date:
        result[FieldKeyConstants.RUN_START_DATE] = backfill.run_start_date.isoformat()
    if backfill.run_end_date:
        result[FieldKeyConstants.RUN_END_DATE] = backfill.run_end_date.isoformat()
    return result


def _get_backfill_by_id(backfill_dag_id):
    """
    get backfill by id with error handling
    """
    backfill, err_msg = validate_and_load_backfill(backfill_dag_id, BACKFILL_COLUMN_NAMES)
    if backfill is None:
        raise NotFound(
            "Backfill not found",
            detail=flat_error_messages(err_msg),
        )
    return backfill


def list_backfills(permitted_dag_ids, data):
    """
    list backfills, filters: orig_dag_id, start_date, end_date, include_deleted
    """
    logging.info(f"list_backfills: permitted_dag_ids: {len(permitted_dag_ids)}, data: {data}")
    curr_dt = timezone.utcnow()

    list_params, err_msg = validate_and_load_list_request(data, dict_get_func)
    if err_msg:
        msg = flat_error_messages(err_msg)
        logging.info(f"error in list_backfills: {msg}")
        raise BadRequest(detail=msg)
    backfills = BackfillModel.list_backfills(
        column_names=BACKFILL_COLUMN_NAMES,
        dag_id=list_params.dag_id,
        include_deleted=list_params.include_deleted,
    )

    filtered_backfills = []
    # filter
    for backfill in backfills:
        # by permitted_dag_ids: only allow backfill id in permitted_dag_ids or origin dag id in permitted_dag_ids
        if not is_backfill_permitted(backfill, permitted_dag_ids):
            continue
        # by include_deleted
        if not list_params.include_deleted and parse_backfill_deleted(backfill, curr_dt) != "":
            continue
        # by start_date and end_date
        if list_params.start_date and list_params.end_date and backfill.logical_dates and not is_any_logical_date_in_range(
                parse_logical_dates_str(backfill.logical_dates),
                list_params.start_date,
                list_params.end_date,
        ):
            continue
        filtered_backfills.append(backfill)

    return [_backfill_to_response(backfill, curr_dt) for backfill in filtered_backfills]


def get_backfill(backfill_dag_id):
    """
    get backfill
    """
    logging.info(f"get_backfill: backfill_dag_id: {backfill_dag_id}")
    curr_dt = timezone.utcnow()
    return _backfill_to_response(_get_backfill_by_id(backfill_dag_id), curr_dt)


def create_backfill(permitted_dag_ids, data, dag_bag, username):
    """
    create backfill: validate, create, return backfill object
    """
    logging.info(f"create_backfill: permitted_dag_ids: {len(permitted_dag_ids)}, data: {data}, username: {username}")
    curr_dt = timezone.utcnow()
    backfill, err_msg = validate_and_load_creation_request(data, dag_bag, curr_dt, username, dict_get_func)
    if err_msg:
        msg = flat_error_messages(err_msg)
        logging.info(f"error in load_from_creation_request: {msg}")
        raise BadRequest(detail=msg)

    # filter by permission, TODO: support RBAC at backfill id level like dag id
    if backfill.dag_id not in permitted_dag_ids:
        raise PermissionDenied()

    BackfillModel.add_backfill(backfill)
    return _backfill_to_response(_get_backfill_by_id(backfill.backfill_dag_id), curr_dt)


def delete_backfill(backfill_dag_id):
    """
    mark delete backfill
    """
    logging.info(f"delete_backfill: backfill_dag_id: {backfill_dag_id}")

    curr_dt = timezone.utcnow()
    backfill = _get_backfill_by_id(backfill_dag_id)
    # check deleted flag and state
    backfill, err_msg = validate_and_load_delete_request(backfill)
    if not backfill:
        raise BadRequest(detail=flat_error_messages(err_msg))

    # mark delete
    BackfillModel.update_delete_date(backfill_dag_id, curr_dt, curr_dt)

    # return backfill
    backfill = _get_backfill_by_id(backfill_dag_id)
    return _backfill_to_response(backfill, curr_dt)


def cancel_backfill(backfill_dag_id, dag_bag):
    """
    cancel submitted backfill or cancel backfill dag runs
    """
    logging.info(f"cancel_backfill: backfill_dag_id: {backfill_dag_id}")
    curr_dt = timezone.utcnow()
    backfill = _get_backfill_by_id(backfill_dag_id)

    if backfill.state == BackfillState.SUBMITTED:
        # cancel submitted backfills before backfill dags were created
        logging.info("cancel submitted backfill")
        deadline_date = curr_dt + timedelta(minutes=BACKFILL_SCHEDULED_DATE_TOLERANCE_MINS)
        backfill, err_msg = validate_and_load_cancel_submitted_backfill_request(backfill, deadline_date)
        if not backfill:
            raise BadRequest(detail=flat_error_messages(err_msg))

        # cancel
        BackfillModel.delete_submitted_backfills(backfill_dag_id, deadline_date, curr_dt)

    else:
        # cancel backfill runs after backfill dags were created
        logging.info("cancel backfill runs")
        backfill, err_msg = validate_and_load_cancel_backfill_runs_request(backfill)
        if not backfill:
            raise BadRequest(detail=flat_error_messages(err_msg))

        # cancel
        from linkedin.airflow.backfill.utils.operations import cancel_backfill_dag_runs, update_states
        processed, err_msg = cancel_backfill_dag_runs(backfill_dag_id, dag_bag)
        if processed == 0:
            raise Unknown(detail=flat_error_messages(err_msg))
        # update state
        backfills = BackfillModel.get_active_backfills(backfill_dag_ids=[backfill_dag_id])
        processed, err_msg = update_states(backfills)
        if processed == 0:
            raise Unknown(detail=flat_error_messages(err_msg))

    # return backfill
    backfill = _get_backfill_by_id(backfill_dag_id)
    return _backfill_to_response(backfill, curr_dt)
