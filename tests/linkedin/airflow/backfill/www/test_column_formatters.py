from linkedin.airflow.backfill.www.column_formatters import (
    duration_f,
    backfill_dag_params_f,
    backfill_dag_link_f,
    dag_code_link_f,
    get_dag_link,
)
from linkedin.airflow.backfill.utils.backfill_state import BackfillState

import pendulum
import pytest


@pytest.fixture()
def mock_url_for(mocker):
    mocker.patch("linkedin.airflow.backfill.www.column_formatters.url_for", return_value='url')


def test_get_dag_link(mock_url_for):
    dag_id = 'my_dag_id'
    assert str(get_dag_link(dag_id)) == '<a href="url">my_dag_id</a>'


def test_dag_code_link(mock_url_for):
    dag_code_link = dag_code_link_f('my_code_link_name')

    # no dag_id
    assert str(dag_code_link({})) == 'None'

    # no backfill id
    assert str(dag_code_link({
        'dag_id': 'my_dag_id',
    })) == '<a href="url">my_dag_id</a>'

    # have all values, state is not submitted
    assert str(dag_code_link({
        'dag_id': 'my_dag_id',
        'backfill_dag_id': 'my_backfill_dag_id',
    })) == '<div style="white-space: pre;"><a href="url">my_dag_id</a> [<a href="url">my_code_link_name</a>]</div>'


def test_backfill_dag_link(mock_url_for):
    backfill_dag_link = backfill_dag_link_f()

    # no id
    assert str(backfill_dag_link({})) == 'None'

    # deleted
    assert str(backfill_dag_link({
        'backfill_dag_id': 'my_backfill_dag_id',
        'deleted': True,
    })) == '<del>my_backfill_dag_id</del>'

    # submitted state, no link
    assert str(backfill_dag_link({
        'backfill_dag_id': 'my_backfill_dag_id',
        'deleted': False,
        'state': BackfillState.SUBMITTED,
    })) == 'my_backfill_dag_id'

    # other states
    assert str(backfill_dag_link({
        'backfill_dag_id': 'my_backfill_dag_id',
        'deleted': False,
        'state': BackfillState.QUEUED,
    })) == '<a href="url">my_backfill_dag_id</a>'


def test_backfill_dag_params(mock_url_for):
    backfill_dag_params = backfill_dag_params_f()

    # no params
    assert str(backfill_dag_params({})) == ' '

    assert str(backfill_dag_params({
        'dag_params': 'my_dag_params',
    })) == 'my_dag_params'


def test_duration(mock_url_for):
    duration = duration_f('start_date', 'end_date')

    # no dates
    assert str(duration({})) == ' '

    # diff available
    start_date = pendulum.datetime(2022, 1, 1, tz="America/Los_Angeles")
    end_date = pendulum.datetime(2022, 6, 1, tz="America/Los_Angeles")
    assert str(duration({
        'start_date': start_date,
        'end_date': end_date,
    })) == '5m:23h'
