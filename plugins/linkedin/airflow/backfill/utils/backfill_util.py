# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow.utils import timezone
import json


def dict_get_func(data, key, default_value=None):
    """
    get function for dict
    """
    return data.get(key, default_value)


def object_get_func(data, key, default_value=None):
    """
    get function for object
    """
    return getattr(data, key, default_value)


def parse_backfill_deleted(backfill, curr_dt=None, get_func=object_get_func):
    """
    backfill is deleted or mark deleted
    """
    if curr_dt is None:
        curr_dt = timezone.utcnow()
    if get_func(backfill, "deleted"):
        return "Deleted"
    if get_func(backfill, "delete_date") <= curr_dt:
        return "ToBeDeleted"
    return ""


def is_backfill_permitted(backfill, permitted_dag_ids):
    """
    backfill dag permitted
    or origin dag permitted if deleted
    """
    return backfill.backfill_dag_id in permitted_dag_ids or backfill.dag_id in permitted_dag_ids


def is_any_logical_date_in_range(logical_dates, start_date, end_date):
    """
    check if any logical date is in the range defined by start_date and end_date inclusive
    """
    for d in logical_dates:
        if start_date <= d and d <= end_date:
            return True
    return False


def parse_logical_dates_str(dates_str):
    """
    parse json str to date list
    """
    dates = json.loads(dates_str)
    return [timezone.parse(date_str) for date_str in dates]


def try_json_to_dict(val):
    """
    parse json to dict, return dict and message if errors
    """
    try:
        data = json.loads(val)
        if isinstance(data, dict):
            return data, ""
        else:
            return None, "not dict"
    except Exception as e:
        return None, str(e)


def flat_error_messages(msg):
    """
    flat a list of message to str
    """
    return msg if isinstance(msg, str) else "\n".join(msg)


def add_error_message(msgs, msg):
    """
    add message or messages to a list
    """
    if isinstance(msg, str):
        msgs.append(msg)
    else:
        msgs.extend(msg)
    return msg


def flash_error_messages(msg, flash_func):
    """
    flash error messages
    """
    if isinstance(msg, str):
        flash_func(msg, 'error')
    else:   # list of messages
        for m in msg:
            flash_func(m, 'error')
