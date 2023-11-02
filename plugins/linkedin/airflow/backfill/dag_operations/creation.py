# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState
from linkedin.airflow.backfill.constants import BACKFILL_USER_DAG_FOLDER, BACKFILL_GET_SUBMITTED_DAGS_LIMIT

import logging
import time
import os
import base64


BACKFILL_CODE_TEMPLATE = """

# apply backfill
from linkedin.airflow.backfill.dag_operations.creation_util import apply_backfill
apply_backfill(
    gl=globals(),
    dag_id="{}",
    backfill_dag_id="{}",
    start_date="{}",
    end_date="{}",
    max_active_runs={},
    dag_params_encoded={},
)
"""


def _create_backfill_dag_file(backfill, file_path):
    """
    generate backfill dag code file
    """

    backfill_code = BACKFILL_CODE_TEMPLATE.format(
        backfill.dag_id,
        backfill.backfill_dag_id,
        backfill.start_date.isoformat(),
        backfill.end_date.isoformat(),
        backfill.max_active_runs,
        base64.b64encode(backfill.dag_params.encode('utf-8')) if backfill.dag_params else "",
    )

    with open(file_path, 'w') as f:
        f.write(backfill.origin_code)
        f.write(backfill_code)


def _process_submitted_backfill(backfill, backfill_user_dag_folder):
    """
    create a backfill dag file
    """
    # create dag file
    backfill_dag_file_path = os.path.join(backfill_user_dag_folder, f"{backfill.backfill_dag_id}.py")
    _create_backfill_dag_file(backfill, backfill_dag_file_path)

    # update info
    BackfillModel.queue(backfill.backfill_dag_id)


def _mark_fail(backfill):
    try:
        BackfillModel.update_run_state(backfill.backfill_dag_id, backfill.updated_at, BackfillState.FAILED, None, None)
    except Exception as e:
        logging.error(f"error in mark fail: {e}")


def process_submitted_backfills():
    """
    loop through submitted backfill requests and create backfill dag files
    """
    try:
        logging.info("processing submitted backfills")

        backfills = BackfillModel.get_submitted_backfills(BACKFILL_GET_SUBMITTED_DAGS_LIMIT)
        # process config files
        processed = 0
        for backfill in backfills:
            logging.info(f"backfill: {backfill.backfill_dag_id}")

            try:
                _process_submitted_backfill(backfill, BACKFILL_USER_DAG_FOLDER)
                processed += 1

                # pause a little bit for db
                time.sleep(0.1)

            except Exception as e:
                logging.error(f"ERROR when processing backfill: {e}")
                import traceback
                logging.error(traceback.format_exc())

        logging.info(f"{processed} submitted backfill(s) processed")
    except Exception as e:
        logging.error(f"ERROR when processing submitted backfills: {e}")
        import traceback
        logging.info(traceback.format_exc())
