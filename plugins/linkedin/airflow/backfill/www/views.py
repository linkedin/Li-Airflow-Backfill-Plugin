# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from flask import flash, g, current_app, redirect
from flask_appbuilder import expose
from flask_appbuilder.actions import action
from airflow.utils import timezone
from airflow.www import auth, utils as wwwutils
from airflow.www.views import AirflowModelView, DagFilter
from airflow.security import permissions
from airflow.configuration import conf as airflow_conf
from markupsafe import Markup, escape
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter

from linkedin.airflow.backfill.www.forms import BackfillCreationForm
from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.constants import (
    BACKFILL_ORIGIN_CODE_SNAPSHOT_DATE_FORMAT,
    BACKFILL_SCHEDULED_DATE_TOLERANCE_MINS,
    FieldKeyConstants,
)
from linkedin.airflow.backfill.utils.request_handling import (
    validate_and_load_creation_request,
    validate_and_load_cancel_submitted_backfill_request,
    validate_and_load_cancel_backfill_runs_request,
    validate_and_load_delete_request,
)
import linkedin.airflow.backfill.www.column_formatters as column_formatters
import linkedin.airflow.backfill.security.permissions as backfill_permissions
from linkedin.airflow.backfill.utils.backfill_util import object_get_func, add_error_message, flash_error_messages

import logging
from datetime import timedelta

# Place in /plugins

# roles to grant backfill menu permissions, string of comma delimited role names
BACKFILL_PERMISSIONS = [('menu_access', 'Backfill'), ('menu_access', 'List Backfills')]


def init_views():
    logging.info("init backfill views")

    # get permission roles from env
    roles = airflow_conf.get("li_backfill", "permitted_roles", fallback="User,Op")
    permitted_roles = [x.strip() for x in roles.split(",")]
    logging.info(f"roles to grant permissions for backfill menus: {permitted_roles}")

    # set permissions
    sm = current_app.appbuilder.sm
    backfill_permissions.grant_backfill_permissions(sm, permitted_roles)


class BackfillView(AirflowModelView):
    """
    View to show records from Backfill table
    """

    route_base = "/backfill"
    default_view = 'list'

    show_template = 'backfill_show.html'

    datamodel = AirflowModelView.CustomSQLAInterface(BackfillModel)  # type: ignore

    class_permission_name = backfill_permissions.RESOURCE_BACKFILL
    method_permission_name = {
        'add': 'create',
        'list': 'read',
        'action_mulmarkdelete': 'operate',
        'action_mulcancel': 'operate',
    }
    base_permissions = [
        permissions.ACTION_CAN_CREATE,
        permissions.ACTION_CAN_READ,
        backfill_permissions.ACTION_CAN_OPERATE,
        permissions.ACTION_CAN_ACCESS_MENU,
    ]

    select_columns = [
        'deleted',
        'state',
        'backfill_dag_id',
        'description',
        'dag_id',
        'start_date',
        'end_date',
        'run_start_date',
        'run_end_date',
        'dag_params',
        'max_active_runs',
        'submitted_by',
        'submitted_at',
        'scheduled_date',
        'delete_date',
        'updated_at',
    ]
    list_columns = [
        'deleted',
        'state',
        'backfill_dag_id',
        'description',
        'dag_id',
        'start_date',
        'end_date',
        'run_start_date',
        'run_end_date',
        'run_duration',
        'dag_params',
        'max_active_runs',
        'submitted_by',
        'submitted_at',
        'scheduled_date',
        'delete_date',
        'updated_at',
    ]
    label_columns = {
        'dag_id': 'Origin Dag',
        'start_date': 'Logical Start Date',
        'end_date': 'Logical End Date',
    }
    search_columns = [
        'deleted',
        'state',
        'backfill_dag_id',
        'description',
        'dag_id',
        'start_date',
        'end_date',
        'scheduled_date',
        'delete_date',
        'submitted_by',
        'submitted_at',
        'updated_at',
    ]
    order_columns = [
        'backfill_dag_id',
        'dag_id',
        'start_date',
        'end_date',
        'scheduled_date',
        'delete_date',
        'submitted_by',
        'submitted_at',
        'updated_at',
    ]
    add_columns = [
        'dag_id',
        'description',
        'start_date',
        'end_date',
        'dag_params',
        'scheduled_date',
        'max_active_runs',
        'delete_duration_days',
        'ignore_overlapping_backfills',
    ]

    add_form = BackfillCreationForm

    base_order = ('submitted_at', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'backfill_dag_id': column_formatters.backfill_dag_link_f(),
        'dag_id': column_formatters.dag_code_link_f('Snapshot'),
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'run_start_date': wwwutils.datetime_f('run_start_date'),
        'run_end_date': wwwutils.datetime_f('run_end_date'),
        'run_duration': column_formatters.duration_f('run_start_date', 'run_end_date'),
        'dag_params': column_formatters.backfill_dag_params_f(),
        'submitted_at': wwwutils.datetime_f('submitted_at'),
        'scheduled_date': wwwutils.datetime_f('scheduled_date'),
        'delete_date': wwwutils.datetime_f('delete_date'),
        'updated_at': wwwutils.datetime_f('updated_at'),
        'deleted': column_formatters.backfill_deleted_f(),
    }

    @action(
        'mulcancel',
        'Cancel Backfills',
        'Are you sure you want to cancel selected backfills?',
        single=False,
    )
    @auth.has_access(
        [
            (backfill_permissions.ACTION_CAN_OPERATE, backfill_permissions.RESOURCE_BACKFILL),
        ]
    )
    def action_mulcancel(self, items):
        """
        cancel backfills
        """
        logging.info('start cancel backfills')
        curr_dt = timezone.utcnow()

        selected_backfills = BackfillModel.get_backfills(
            [item.backfill_dag_id for item in items],
            BackfillView.select_columns,
        )

        err_msgs = []
        cancel_submitted_backfills = []
        cancel_run_backfills = []
        for item in selected_backfills:
            if item.state == BackfillState.SUBMITTED:
                logging.info("cancel submitted backfill")
                deadline_date = curr_dt + timedelta(minutes=BACKFILL_SCHEDULED_DATE_TOLERANCE_MINS)
                backfill, err_msg = validate_and_load_cancel_submitted_backfill_request(item, deadline_date)
                if not backfill:
                    add_error_message(err_msgs, err_msg)
                    continue
                cancel_submitted_backfills.append(backfill.backfill_dag_id)
            else:
                logging.info("cancel backfill runs")
                backfill, err_msg = validate_and_load_cancel_backfill_runs_request(item)
                if not backfill:
                    add_error_message(err_msgs, err_msg)
                    continue
                cancel_run_backfills.append(backfill.backfill_dag_id)

        processed = len(cancel_submitted_backfills)
        # cancel submitted
        if cancel_submitted_backfills:
            # mark delete
            BackfillModel.delete_submitted_backfills(cancel_submitted_backfills, deadline_date, curr_dt)

        if cancel_run_backfills:
            # cancel runs
            from linkedin.airflow.backfill.utils.operations import cancel_backfill_dag_runs, update_states
            processed_cancel_run, err_msg = cancel_backfill_dag_runs(cancel_run_backfills, current_app.dag_bag)
            if err_msg:
                add_error_message(err_msgs, err_msg)
            if processed_cancel_run > 0:
                # update state
                backfills = BackfillModel.get_active_backfills(backfill_dag_ids=cancel_run_backfills)
                processed_update_state, err_msg = update_states(backfills)
                if err_msg:
                    add_error_message(err_msgs, err_msg)
                processed += processed_update_state

        if processed > 0:
            flash(f'cancelled {processed} backfills successfully', 'message')
        failed = len(selected_backfills) - processed
        if failed > 0:
            flash(f'failed to cancel {failed} backfills', 'error')
            flash_error_messages(err_msgs, flash)

        self.update_redirect()
        return redirect(self.get_redirect())

    @action(
        'mulmarkdelete',
        'Delete Backfill Dags',
        'Are you sure you want to delete selected backfill Dags?',
        single=False,
    )
    @auth.has_access(
        [
            (backfill_permissions.ACTION_CAN_OPERATE, backfill_permissions.RESOURCE_BACKFILL),
        ]
    )
    def action_mulmarkdelete(self, items):
        """
        mark delete backfill dags
        """
        logging.info('start schedule to delete backfill dags')
        curr_dt = timezone.utcnow()

        selected_backfills = BackfillModel.get_backfills(
            [item.backfill_dag_id for item in items],
            BackfillView.select_columns,
        )

        err_msgs = []
        backfill_dag_ids = []
        for item in selected_backfills:
            backfill, err_msg = validate_and_load_delete_request(item)
            if err_msg:
                add_error_message(err_msgs, err_msg)
                continue
            backfill_dag_ids.append(item.backfill_dag_id)

        if backfill_dag_ids:
            BackfillModel.update_delete_date(backfill_dag_ids, curr_dt, curr_dt)

        processed = len(backfill_dag_ids)
        if processed > 0:
            flash(f'cancelled {processed} backfills successfully', 'message')
        failed = len(selected_backfills) - processed
        if failed > 0:
            flash(f'failed to delete {failed} backfills', 'error')
            flash_error_messages(err_msgs, flash)

        self.update_redirect()
        return redirect(self.get_redirect())

    def pre_add(self, item):
        """
        adjust values before add
        """
        curr_dt = timezone.utcnow()
        backfill, _ = validate_and_load_creation_request(
            data=item,
            dag_bag=current_app.dag_bag,
            curr_dt=curr_dt,
            username=g.user.username,
            get_func=object_get_func,
        )
        # update values accordingly
        # backfill_dag_id
        setattr(item, FieldKeyConstants.BACKFILL_DAG_ID, backfill.backfill_dag_id)
        # logical_dates
        setattr(item, FieldKeyConstants.LOGICAL_DATES, backfill.logical_dates)
        # delete_date
        setattr(item, FieldKeyConstants.DELETE_DATE, backfill.delete_date)
        # state
        setattr(item, FieldKeyConstants.STATE, backfill.state)
        # relative_fileloc
        setattr(item, FieldKeyConstants.RELATIVE_FILELOC, backfill.relative_fileloc)
        # origin_code
        setattr(item, FieldKeyConstants.ORIGIN_CODE, backfill.origin_code)
        # submitted_by
        setattr(item, FieldKeyConstants.SUBMITTED_BY, backfill.submitted_by)
        # submitted_at
        setattr(item, FieldKeyConstants.SUBMITTED_AT, backfill.submitted_at)
        # updated_at
        setattr(item, FieldKeyConstants.UPDATED_AT, backfill.updated_at)

        # remove UI only values
        delattr(item, FieldKeyConstants.DELETE_DURATION_DAYS)
        delattr(item, FieldKeyConstants.IGNORE_OVERLAPPING_BACKFILLS)

    def post_add(self, item):
        """
        after backfill creation
        """
        dag_id = getattr(item, FieldKeyConstants.DAG_ID)
        backfill_dag_id = getattr(item, FieldKeyConstants.BACKFILL_DAG_ID)
        flash(
            f"The backfill for {dag_id} was submitted successfully. A new DAG {backfill_dag_id} will be created in a few minutes",
            "success",
        )

    @expose('/<string:backfill_dag_id>/code', methods=['GET'])
    @auth.has_access(
        [
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE),
        ]
    )
    def code(self, backfill_dag_id):
        """
        Origin DAG code
        """
        all_errors = ""
        backfill = None

        try:
            backfill = BackfillModel.get_backfill_code(backfill_dag_id)
            html_code = Markup(highlight(backfill.origin_code, lexers.PythonLexer(), HtmlFormatter(linenos=True)))
        except Exception as e:
            all_errors += (
                "Exception encountered during "
                f"dag_id retrieval/dag retrieval fallback/code highlighting:\n\n{e}\n"
            )
            html_code = Markup('<p>Failed to load DAG file Code.</p><p>Details: {}</p>').format(
                escape(all_errors)
            )

        snapshot_date = backfill.submitted_at.strftime(BACKFILL_ORIGIN_CODE_SNAPSHOT_DATE_FORMAT)
        return self.render_template(
            'dag_code.html',
            html_code=html_code,
            backfill_dag_id=backfill_dag_id,
            title=f'Dag {backfill.dag_id} Code Snapshot at {snapshot_date} for Backfill {backfill_dag_id}' if backfill else '',
            wrapped=airflow_conf.getboolean('webserver', 'default_wrap'),
        )
