import random
from tests.linkedin.airflow.backfill.dags import test_dag_utc as test_dag

import os


class TestObj:
    """a obj to have any attr"""
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


def assert_obj_equals(o1, o2, attr_names):
    for attr_name in attr_names:
        assert getattr(o1, attr_name) == getattr(o2, attr_name)


def get_random_string(candidates, k=None):
    return ''.join(random.choices(candidates, k=k))


def get_test_dag_path():
    return os.path.realpath(test_dag.__file__)


def get_test_dag():
    return test_dag.dag


def assert_dag_equals(o1, o2):
    assert_obj_equals(o1, o2, ['dag_id'])
    # TODO: add more asserts when needed
