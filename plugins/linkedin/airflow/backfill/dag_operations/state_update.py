# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.operations import update_states

import logging


def update_backfill_states(state_list):
    """
    update state according to dag runs.
    Due to possible dag operations from users, state can change at anytime before dag deletion.
    The most frequent changes states are queued and running , other states may never change.
    Therefore, different schedules may be used to check for different states.
    :param: state_list: a list of states to check for
    """
    try:
        logging.info(f'start update backfill state, state_list={state_list}')

        backfills = BackfillModel.get_active_backfills(state_list=state_list)
        logging.info(f"got {len(backfills)} backfills")
        processed = update_states(backfills)

        logging.info(f"processed {processed} backfill(s)")
    except Exception as e:
        logging.error(f"ERROR in update_state: {e}")
        import traceback
        logging.info(traceback.format_exc())
