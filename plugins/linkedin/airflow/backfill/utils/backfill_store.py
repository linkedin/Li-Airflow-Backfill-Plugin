# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

from airflow.utils import timezone
from airflow.configuration import conf


class BackfillStoreBase:
    """
    store backfill Dag files
    TODO: add more store logic besides custom dag file name
    """

    def generate_backfill_dag_id(self, dag_id, dt=None):
        """Generates the backfill DAG's name."""
        if dt is None:
            dt = timezone.utcnow()
        ts = dt.strftime("%Y%m%d_%H%M%S")
        return f"{dag_id}_backfill_{ts}"


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
