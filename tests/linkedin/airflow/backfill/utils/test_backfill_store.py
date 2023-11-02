from linkedin.airflow.backfill.utils.backfill_store import resolve_backfill_store_backend, BackfillStoreBase
from airflow.configuration import conf

import pytest
from unittest.mock import Mock


class MyBackfillStore(BackfillStoreBase):
    pass


class FakeBackfillStore:
    pass


def test_resolve_backfill_store_backend():
    # 1. custom store
    mocked_getimport = conf.getimport = Mock(return_value=MyBackfillStore)
    cls = resolve_backfill_store_backend()
    assert issubclass(cls, MyBackfillStore)
    mocked_getimport.close()

    # 2. fallback
    mocked_getimport = conf.getimport = Mock(return_value=None)
    cls = resolve_backfill_store_backend()
    assert issubclass(cls, BackfillStoreBase)
    mocked_getimport.close()

    # 3. type error
    mocked_getimport = conf.getimport = Mock(return_value=FakeBackfillStore)
    with pytest.raises(TypeError):
        resolve_backfill_store_backend()
    mocked_getimport.close()
