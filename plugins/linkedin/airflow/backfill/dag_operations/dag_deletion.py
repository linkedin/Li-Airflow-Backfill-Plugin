# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.utils.operations import pause_dag
from linkedin.airflow.backfill.utils.operations import delete_dag
from linkedin.airflow.backfill.constants import BACKFILL_USER_DAG_FOLDER

import logging
import os
import time


def _delete_backfill_dag(backfill_dag_folder, backfill):
    """
    delete backfill dag file and dag metadata
    """

    # check state
    if not BackfillState.is_deletable(backfill.state):
        logging.info('backfill state is not deletable, skipping')
        return

    # pause dag
    pause_dag(backfill.backfill_dag_id)

    # delete dag file
    file_path = os.path.join(backfill_dag_folder, f"{backfill.backfill_dag_id}.py")
    if os.path.exists(file_path):
        os.remove(file_path)

    delete_dag(dag_id=backfill.backfill_dag_id)

    # set delete flag to backfill table
    BackfillModel.delete_dag(backfill.backfill_dag_id)


def delete_backfill_dags():
    """
    delete backfill dags:
        pause dag
        delete dag file
        delete dag
        set deleted flag
    """
    try:
        logging.info('start delete mark-deleted backfills')

        backfill_dag_folder = BACKFILL_USER_DAG_FOLDER
        backfills = BackfillModel.get_deletable_backfills()
        processed = 0
        for backfill in backfills:
            try:
                logging.info(f"backfill: {backfill.backfill_dag_id}, state: {backfill.state}")

                _delete_backfill_dag(backfill_dag_folder, backfill)
                processed += 1

                # pause a little bit
                time.sleep(0.1)
            except Exception as e:
                logging.error(f"ERROR when processing backfill: {e}")
                import traceback
                logging.error(traceback.format_exc())

        logging.info(f"processed {processed} backfill(s)")
    except Exception as e:
        logging.error(f"ERROR in delete_backfill_dags: {e}")
        import traceback
        logging.info(traceback.format_exc())
