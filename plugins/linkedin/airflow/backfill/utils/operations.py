# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow.utils import timezone
from airflow.utils.state import State
from airflow.models import DagModel, DagRun
from airflow.api.common.mark_tasks import set_dag_run_state_to_failed as airflow_set_dag_run_state_to_failed
from airflow.api.common.delete_dag import delete_dag as airflow_delete_dag

from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.constants import BACKFILL_DAG_CREATION_WAIT_MINS
from linkedin.airflow.backfill.utils.backfill_util import parse_logical_dates_str

import logging
import datetime


def _parse_dag_runs(logical_dates_set, dag_runs):
    """
    parse dag runs to have state and run timestamps
    """
    # remove non logical date runs and group runs by logical dates
    date_dict = {}
    for dag_run in dag_runs:
        logical_date = dag_run.logical_date
        if logical_date not in logical_dates_set:
            continue
        if logical_date in date_dict:
            date_dict[logical_date].append(dag_run)
        else:
            date_dict[logical_date] = [dag_run]

    # check valid dag runs
    success_count = 0
    failed_count = 0
    finished_count = 0
    min_date = None
    max_date = None
    for dated_dag_runs in date_dict.values():
        # check all runs in current date
        date_min_date = None
        date_max_date = None
        date_state = None
        for dag_run in dated_dag_runs:
            # min date: min out of all runs
            if not date_min_date or (dag_run.start_date and dag_run.start_date < date_min_date):
                date_min_date = dag_run.start_date

            # running if any is running, then no need to process further
            if date_state == BackfillState.RUNNING:
                continue

            if dag_run.get_state() in State.finished:
                # use the run with max end date
                if not date_state or (dag_run.end_date and dag_run.end_date > date_max_date):
                    date_state = BackfillState.SUCCESS if dag_run.get_state() in State.success_states else BackfillState.FAILED
                    date_max_date = dag_run.end_date
            else:
                date_state = BackfillState.RUNNING
                date_max_date = None
        # runs in the same logical dates were combined

        if date_state == BackfillState.SUCCESS:
            success_count += 1
            finished_count += 1
        elif date_state == BackfillState.FAILED:
            failed_count += 1
            finished_count += 1

        if not min_date or (date_min_date and date_min_date < min_date):
            min_date = date_min_date
        if not max_date or (date_max_date and date_max_date > max_date):
            max_date = date_max_date

    state = BackfillState.RUNNING
    if failed_count > 0:
        state = BackfillState.FAILED
    elif success_count == len(logical_dates_set):
        state = BackfillState.SUCCESS

    if len(logical_dates_set) - finished_count > 0:
        # not finished yet, no max_date
        max_date = None

    return state, min_date, max_date


def _get_dag_runs(dag_ids):
    """
    get a dict of dig_id to dag runs
    """
    dag_runs = DagRun.find(dag_id=dag_ids, no_backfills=True)
    r = {}
    for dag_run in dag_runs:
        if dag_run.dag_id in r:
            r[dag_run.dag_id].append(dag_run)
        else:
            r[dag_run.dag_id] = [dag_run]
    return r


def _update_state(backfill, is_paused, dag_runs):
    """
    update backfill state according to dag run states
    """
    if not BackfillState.is_updatable(backfill.state):
        logging.info(f'state {backfill.state} is not updatable, skipping')
        return

    # backfill dag should exist
    dag_model = DagModel.get_current(backfill.backfill_dag_id)
    if not dag_model:
        # mark fail if dag was not created after BACKFILL_DAG_CREATION_WAIT_MINS
        if timezone.utcnow() - backfill.updated_at > datetime.timedelta(minutes=BACKFILL_DAG_CREATION_WAIT_MINS):
            logging.info(f"backfill dag {backfill.backfill_dag_id} was not created, mark as fail")
            BackfillModel.update_run_state(backfill.backfill_dag_id, backfill.updated_at, BackfillState.FAILED, None, None)
        else:
            logging.info(f"backfill dag {backfill.backfill_dag_id} was not created yet, skip")
        return

    curr_logical_dates = parse_logical_dates_str(backfill.logical_dates) if backfill.logical_dates else []
    curr_logical_dates_set = set(curr_logical_dates) if curr_logical_dates else set()
    state, min_date, max_date = _parse_dag_runs(curr_logical_dates_set, dag_runs)

    if is_paused and not BackfillState.is_finished(state):
        state = BackfillState.PAUSED

    run_start_date = min_date
    run_end_date = max_date if BackfillState.is_finished(state) else None

    # persist
    BackfillModel.update_run_state(backfill.backfill_dag_id, backfill.updated_at, state, run_start_date, run_end_date)


def update_states(backfills):
    """
    update backfills' states according to the backfill Dag run states
    """
    if not backfills:
        logging.info("no backfill to update, quit")
        return 0

    """update backfill states"""
    backfill_dag_ids = [backfill.backfill_dag_id for backfill in backfills]

    # get paused dags
    paused_dag_ids_set = DagModel.get_paused_dag_ids(backfill_dag_ids)
    logging.info(f"got {len(paused_dag_ids_set)} paused dag ids")

    # get dag runs
    dag_runs_dict = _get_dag_runs(backfill_dag_ids)
    logging.info(f"got {len(dag_runs_dict)} dag_runs_dict")

    # process backfills
    processed = 0
    error_msg = []
    for backfill in backfills:
        logging.info(f"backfill: {backfill.backfill_dag_id}, state: {backfill.state}")
        try:
            dag_runs = dag_runs_dict[backfill.backfill_dag_id] if backfill.backfill_dag_id in dag_runs_dict else []
            _update_state(backfill, backfill.backfill_dag_id in paused_dag_ids_set, dag_runs)
            processed += 1
        except Exception as e:
            import traceback
            error_msg.append(f"error update state backfill {backfill.backfill_dag_id}: {e}")
            error_msg.append(traceback.format_exc())
    return processed, error_msg


def pause_dag(dag_id):
    """
    pause a dag
    """
    dag_model = DagModel.get_current(dag_id)
    if dag_model:
        dag_model.set_is_paused(is_paused=True)


def delete_dag(dag_id):
    """
    delete dag (subdags also deleted, this is not expected but subdag is depracating and not supported in backfill)
    """
    try:
        airflow_delete_dag(dag_id)
        return True
    except Exception as e:
        logging.error(f"ERROR delete dag {dag_id}: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False


def _cancel_backfill_dag_run(backfill_dag_id, dag_runs, dag_bag):
    """
    cancel a backfill dag run
        pause backfill dags
        mark unfinished dag runs as failed
    """
    # pause dags
    pause_dag(backfill_dag_id)

    # set unfinished dag runs to fail
    for dag_run in dag_runs:
        if dag_run.get_state() in State.unfinished:
            # set dag run to fail
            dag = dag_bag.get_dag(backfill_dag_id)
            airflow_set_dag_run_state_to_failed(dag=dag, run_id=dag_run.run_id, commit=True)


def cancel_backfill_dag_runs(backfill_dag_ids, dag_bag):
    """
    cancel backfill dag runs
    """
    if not backfill_dag_ids:
        logging.info("no backfill to cancel, quit")
        return 0

    if isinstance(backfill_dag_ids, str):
        backfill_dag_ids = [backfill_dag_ids]

    dag_runs_dict = _get_dag_runs(backfill_dag_ids)
    processed = 0
    error_msg = []
    for backfill_dag_id in backfill_dag_ids:
        logging.info(f"backfill: {backfill_dag_id}")
        try:
            dag_runs = dag_runs_dict[backfill_dag_id] if backfill_dag_id in dag_runs_dict else []
            _cancel_backfill_dag_run(backfill_dag_id, dag_runs, dag_bag)
            processed += 1
        except Exception as e:
            import traceback
            error_msg.append(f"error cancel backfill {backfill_dag_id} runs: {e}")
            error_msg.append(traceback.format_exc())
    return processed, error_msg
