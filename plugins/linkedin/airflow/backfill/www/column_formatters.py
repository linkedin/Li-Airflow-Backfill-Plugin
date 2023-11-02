# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from markupsafe import Markup
from flask import url_for
from airflow.utils.timezone import td_format
from airflow.utils import timezone

from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.utils.backfill_util import parse_backfill_deleted, dict_get_func


def get_dag_link(dag_id):
    """
    Generate DAG link
    """
    url = url_for('Airflow.grid', dag_id=dag_id)
    return Markup('<a href="{}">{}</a>').format(url, dag_id)


def dag_code_link_f(code_link_name):
    """
    Generates a function to get DAG link and code link
    """

    def dag_code_link(attr):
        dag_id = attr.get('dag_id')
        backfill_dag_id = attr.get('backfill_dag_id')
        if not dag_id:
            return Markup('None')
        dag_html = '<a href="{}">{}</a>'
        dag_url = url_for('Airflow.grid', dag_id=dag_id)
        if not backfill_dag_id:
            # code is not available before dag creation
            return Markup(dag_html).format(dag_url, dag_id)
        else:
            code_url = url_for('BackfillView.code', backfill_dag_id=backfill_dag_id)
            # nowrap
            return Markup('<div style="white-space: pre;">' + dag_html + ' [<a href="{}">{}</a>]</div>').format(
                dag_url, dag_id, code_url, code_link_name
            )

    return dag_code_link


def backfill_dag_link_f():
    """
    Generated a function to get backfill DAG link
    """
    def backfill_dag_link(attr):
        """
        get backfill link
        """

        backfill_dag_id = attr.get('backfill_dag_id')
        if not backfill_dag_id:
            return Markup('None')
        if attr.get('deleted'):
            return Markup('<del>{}</del>').format(backfill_dag_id)
        elif not BackfillState.is_created(attr.get('state')):
            # backfill dag is not available before dag creation
            return Markup('{}').format(backfill_dag_id)
        else:
            return get_dag_link(backfill_dag_id)

    return backfill_dag_link


def backfill_dag_params_f():
    """
    Generated a function to get backfill dag params
    """
    def backfill_dag_params(attr):
        """
        Return backfill dag params
        """

        dag_params = attr.get('dag_params')
        # nowrap
        return Markup('{}').format(dag_params) if dag_params else Markup(' ')

    return backfill_dag_params


def duration_f(start_date_name, end_date_name):
    """
    Generated a function to calculate run duration
    """

    def duration(attr):
        start_date = attr.get(start_date_name)
        end_date = attr.get(end_date_name)

        difference = ' '
        if start_date and end_date:
            difference = td_format(end_date - start_date)

        return Markup('{}').format(difference)

    return duration


def backfill_deleted_f():
    """
    Generate a function to show deleted
    """

    def backfill_deleted(attr):
        curr_dt = timezone.utcnow()
        val = parse_backfill_deleted(attr, curr_dt, dict_get_func)
        return Markup(val) if val else Markup(' ')

    return backfill_deleted
