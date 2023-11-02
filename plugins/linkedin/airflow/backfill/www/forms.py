# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow.utils import timezone
from airflow.models import DagModel
from airflow.utils.session import create_session
from flask_appbuilder.forms import DynamicForm
from flask_appbuilder.fieldwidgets import BS3TextAreaFieldWidget, BS3TextFieldWidget, Select2Widget
from flask_babel import lazy_gettext
from wtforms.fields import IntegerField, SelectField, StringField, TextAreaField, BooleanField
from wtforms.validators import InputRequired, Optional, NumberRange, Length
from airflow.www.forms import DateTimeWithTimezoneField
from airflow.www.widgets import AirflowDateTimePickerWidget

from flask import flash, g, current_app

from linkedin.airflow.backfill.constants import BACKFILL_MAX_ACTIVE_DAG_RUNS
from linkedin.airflow.backfill.utils.request_handling import validate_and_load_creation_request
from linkedin.airflow.backfill.utils.backfill_util import flash_error_messages


class BackfillCreationForm(DynamicForm):
    """
    Form for creating backfills
    """

    dag_id = SelectField(
        lazy_gettext('Select DAG'),
        validators=[Length(min=1, message='Select DAG')],
        widget=Select2Widget(style="width: 100%;"),
    )
    description = StringField(
        lazy_gettext('Description'),
        validators=[Optional()],
        widget=BS3TextFieldWidget()
    )
    start_date = DateTimeWithTimezoneField(
        lazy_gettext('Start Date'),
        validators=[InputRequired()],
        widget=AirflowDateTimePickerWidget()
    )
    end_date = DateTimeWithTimezoneField(
        lazy_gettext('End Date'),
        validators=[InputRequired()],
        widget=AirflowDateTimePickerWidget())
    ignore_overlapping_backfills = BooleanField(
        lazy_gettext('Ignore Overlapping Backfills'),
        default=False,
        validators=[Optional()],
    )
    dag_params = TextAreaField(
        lazy_gettext('Additional DAG Params (must be a dict object)'),
        default='{}',
        description='Add more params to dag\'s existing params. To access params in your DAG use "dag.params", or "params". '
                    + '"params" are the final params to run a task, which combines dag_run.conf, dag.params and task.params',
        validators=[Optional()], widget=BS3TextAreaFieldWidget()
    )
    scheduled_date = DateTimeWithTimezoneField(
        lazy_gettext('Schedule Backfill to Run at'),
        description='Schedule to run backfill at future time or current datetime for immediately start',
        default=timezone.utcnow,    # use function to avoid default being fixed to compile time
        validators=[InputRequired()],
        widget=AirflowDateTimePickerWidget()
    )
    max_active_runs = IntegerField(
        lazy_gettext(f'Max Concurrent DAG Runs (1-{BACKFILL_MAX_ACTIVE_DAG_RUNS})'),
        default=1,
        validators=[
            InputRequired(),
            NumberRange(min=1, max=BACKFILL_MAX_ACTIVE_DAG_RUNS),
        ],
        widget=BS3TextFieldWidget()
    )
    delete_duration_days = IntegerField(
        lazy_gettext('Delete Created Backfill DAG after (days)'),
        default=30,
        validators=[
            NumberRange(min=1),
        ],
        widget=BS3TextFieldWidget()
    )

    @classmethod
    def refresh(cls):
        """
        create form
        """
        form = super().refresh()

        # populate dag id choices
        editable_dag_ids = current_app.appbuilder.sm.get_editable_dag_ids(g.user)
        with create_session() as session:
            dags = session.query(
                DagModel.dag_id
            ).filter(
                ~DagModel.is_subdag,
                DagModel.is_active,
                DagModel.dag_id.in_(editable_dag_ids),
            ).all()

        dag_ids = [dag.dag_id for dag in dags]
        # sort by dag id, case insensitive
        dag_ids = sorted(dag_ids, key=str.lower)
        # add empty choice
        dag_ids.insert(0, '')
        form.dag_id.choices = [(x, x) for x in dag_ids]

        return form

    def validate(self):
        """
        validate before submit
        """
        if not super().validate():
            return False

        curr_dt = timezone.utcnow()
        _, err_msg = validate_and_load_creation_request(
            data=self,
            dag_bag=current_app.dag_bag,
            curr_dt=curr_dt,
            username=g.user.username,
            get_func=lambda data, key, default_value=None: data[key].data,
        )
        if err_msg:
            flash_error_messages(err_msg, flash)
            return False

        return True
