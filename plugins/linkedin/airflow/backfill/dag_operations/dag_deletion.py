# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.utils.operations import pause_dag
from linkedin.airflow.backfill.utils.operations import delete_dag

import logging
import time


def _delete_backfill_dag(backfill, store):
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
    store.delete_dag_file(backfill)

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
        from linkedin.airflow.backfill.utils.backfill_store import backfill_store
        logging.info('start delete mark-deleted backfills')

        backfills = BackfillModel.get_deletable_backfills()
        processed = 0
        for backfill in backfills:
            try:
                logging.info(f"backfill: {backfill.backfill_dag_id}, state: {backfill.state}")

                _delete_backfill_dag(backfill, backfill_store)
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
