# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow.utils import timezone
from airflow.configuration import conf
from linkedin.airflow.backfill.constants import AIRFLOW_FOLDER
import os


class BackfillStoreBase:
    """
    store backfill Dag files
    TODO: add more store logic besides custom dag file name
    """

    BACKFILL_USER_DAG_FOLDER = os.path.join(AIRFLOW_FOLDER, "dags/backfill_user_dags")

    def generate_backfill_dag_id(self, dag_id, dt=None):
        """Generates the backfill DAG's name."""
        if dt is None:
            dt = timezone.utcnow()
        ts = dt.strftime("%Y%m%d_%H%M%S")
        return f"{dag_id}_backfill_{ts}"

    def persist_dag_file(self, backfill, backfill_dag_code):
        """
        persist dag file to BACKFILL_USER_DAG_FOLDER/backfill_dag_id.py
        """
        # make sure the folder exists
        os.makedirs(self.BACKFILL_USER_DAG_FOLDER, exist_ok=True)
        backfill_dag_path = self._get_backfill_dag_path(backfill)
        with open(backfill_dag_path, 'w') as f:
            f.write(backfill_dag_code)

    def delete_dag_file(self, backfill):
        """
        delete backfill dag
        """
        backfill_dag_path = self._get_backfill_dag_path(backfill)
        if os.path.exists(backfill_dag_path):
            os.remove(backfill_dag_path)

    def _get_backfill_dag_path(self, backfill):
        return os.path.join(self.BACKFILL_USER_DAG_FOLDER, f"{backfill.backfill_dag_id}.py")


def resolve_backfill_store_backend():
    """
    Resolves custom BackfillStore class
    """
    clazz = conf.getimport("li_backfill", "backfill_store", fallback=None)
    if not clazz:
        return BackfillStoreBase
    if not issubclass(clazz, BackfillStoreBase):
        raise TypeError(
            f"Your custom BackfillStore class {clazz.__name__} is not a subclass of {BackfillStoreBase.__name__}."
        )
    return clazz


BackfillStore = resolve_backfill_store_backend()
backfill_store = BackfillStore()
