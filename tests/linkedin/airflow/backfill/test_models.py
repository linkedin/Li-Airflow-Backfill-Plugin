from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from airflow.utils import timezone

from linkedin.airflow.backfill.models.backfill import BackfillModel
from linkedin.airflow.backfill.utils.backfill_state import BackfillState

from tests.linkedin.airflow.backfill.testutils import get_random_string

import pytest
import random
import string
import json
from datetime import timedelta
import pendulum


@pytest.fixture(scope='session')
def engine():
    """yields a SQLAlchemy engine which is suppressed after the test session"""
    engine_ = create_engine('sqlite:///:memory:')
    yield engine_
    engine_.dispose()


@pytest.fixture(scope='session')
def tables(engine):
    """create tables"""
    BackfillModel.__table__.create(engine, checkfirst=True)
    yield
    BackfillModel.__table__.drop(engine)


@pytest.fixture(scope='function')
def db_session(engine, tables):
    """yields a SQLAlchemy connection which is rollbacked after the test"""
    connection = engine.connect()
    # use the connection with the already started transaction
    session = Session(bind=connection)

    try:
        yield session
    finally:
        session.rollback()
        session.close()
        # put back the connection to the connection pool
        connection.close()


def get_random_backfill(start_date=None, end_date=None):
    if not start_date:
        start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    if not end_date:
        end_date = pendulum.datetime(2022, 6, 2, tz='UTC')
    curr_dt = timezone.utcnow()

    return BackfillModel(
        backfill_dag_id=get_random_string(string.ascii_letters, k=10),
        dag_id=get_random_string(string.ascii_letters, k=10),
        state=random.choices(list(BackfillState))[0],
        description=get_random_string(string.ascii_lowercase, k=50),
        start_date=start_date,
        end_date=end_date,
        logical_dates=json.dumps([start_date.isoformat(), end_date.isoformat()]),
        dag_params=json.dumps({'key1': 'value1'}),
        max_active_runs=random.randrange(1, 3),
        scheduled_date=curr_dt,
        delete_date=curr_dt + timedelta(hours=1),
        relative_fileloc=get_random_string(string.ascii_lowercase, k=5),
        origin_code=get_random_string(string.ascii_lowercase, k=50),
        submitted_at=curr_dt,
        submitted_by=get_random_string(string.ascii_lowercase, k=5),
        updated_at=curr_dt,
        deleted=False,
        run_start_date=curr_dt + timedelta(minutes=2),
        run_end_date=curr_dt + timedelta(hours=2),
    )


def test_get_backfills(db_session):

    column_names = ['backfill_dag_id']

    # case: not found
    assert not BackfillModel.get_backfills(['my_id'], column_names, db_session)

    # add backfill
    o = get_random_backfill()
    db_session.add(o)

    # case: empty dag_ids
    assert not BackfillModel.get_backfills([], column_names, db_session)

    # case: found
    r = BackfillModel.get_backfills([o.backfill_dag_id], column_names, db_session)
    assert len(r) == 1
    assert r[0].backfill_dag_id == o.backfill_dag_id
    # other columns are not loaded
    with pytest.raises(AttributeError) as exc:
        r[0].dag_id
        assert 'dag_id' in str(exc.value)


def test_get_backfill_code(db_session):
    # not found
    assert BackfillModel.get_backfill_code('my_id', db_session) is None

    o = get_random_backfill()
    db_session.add(o)
    r = BackfillModel.get_backfill_code(o.backfill_dag_id, db_session)
    assert r is not None
    assert r.dag_id == o.dag_id
    assert r.origin_code == o.origin_code
    assert r.submitted_at == o.submitted_at


def test_get_overlapping_backfills(db_session):

    def assert_backfill_equal(o1, o2):
        assert o2.backfill_dag_id == o1.backfill_dag_id
        assert o2.start_date == o1.start_date
        assert o2.end_date == o1.end_date

    curr_dt = timezone.utcnow()
    loopback_date = curr_dt - timedelta(days=3)

    # no data
    assert not BackfillModel.get_overlapping_backfills('my_id', curr_dt, curr_dt, loopback_date, db_session)

    # add one backfill
    o1 = get_random_backfill()
    o1.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o1.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    o1.submitted_at = curr_dt - timedelta(2)
    db_session.add(o1)

    o2 = get_random_backfill()
    o2.backfill_dag_id = o1.backfill_dag_id + 'my_id'
    o2.dag_id = o1.dag_id

    # overlapping case: left overlap: o2s  o1s  o2e  o1e
    o2.start_date = pendulum.datetime(2022, 5, 25, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 3, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: left start_date overlap: o2s  o1s=o2e  o1e
    o2.start_date = pendulum.datetime(2022, 5, 25, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: right overlap: o1s  o2s  o1e  o2e
    o2.start_date = pendulum.datetime(2022, 6, 3, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 10, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: right end_date overlap: o1s  o2s=o1e  o2e
    o2.start_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 10, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: within overlap: o1s  o2s  o2e  o1e
    o2.start_date = pendulum.datetime(2022, 6, 3, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 4, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: within start_date edge overlap: o1s=o2s  o2e  o1e
    o2.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 4, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: within end_date edge overlap: o1s  o2s  o2e=o1e
    o2.start_date = pendulum.datetime(2022, 6, 3, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: equal overlap: o1s=o2s  o2e=o1e
    o2.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: include overlap: o2s  o1s  o1e  o2e
    o2.start_date = pendulum.datetime(2022, 5, 25, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 10, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: include start_date edge overlap: o2s=o1s  o1e  o2e
    o2.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 10, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # overlapping case: include end_date edge overlap: o2s  o1s  o1e=o2e
    o2.start_date = pendulum.datetime(2022, 5, 25, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    r = BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)
    assert len(r) == 1
    assert_backfill_equal(r[0], o1)

    # non-overlapping case: left: o2s  o2e  o1s  o1e
    o2.start_date = pendulum.datetime(2022, 5, 20, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 5, 25, tz='UTC')
    assert not BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)

    # non-overlapping case: right: o1s  o1e  o2s  o2e
    o2.start_date = pendulum.datetime(2022, 6, 10, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 15, tz='UTC')
    assert not BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)

    # non-overlapping case: different dag id
    o2.dag_id = o1.dag_id + 'my_id'
    o2.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    assert not BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)

    # non-overlapping case: older than loopback backfill
    o2.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    new_loopback_date = curr_dt - timedelta(days=1)
    assert not BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, new_loopback_date, db_session)

    # non-overlapping case: deleting
    # persist deleting backfill
    o1 = get_random_backfill()
    o1.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o1.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    o1.delete_date = curr_dt - timedelta(hours=1)
    db_session.add(o1)

    o2 = get_random_backfill()
    o2.backfill_dag_id = o1.backfill_dag_id + 'my_id'
    o2.dag_id = o1.dag_id

    o2.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    assert not BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)

    # non-overlapping case: deleted
    # persist deleting backfill
    o1 = get_random_backfill()
    o1.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o1.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    o1.deleted = True
    db_session.add(o1)

    o2 = get_random_backfill()
    o2.backfill_dag_id = o1.backfill_dag_id + 'my_id'
    o2.dag_id = o1.dag_id

    o2.start_date = pendulum.datetime(2022, 6, 1, tz='UTC')
    o2.end_date = pendulum.datetime(2022, 6, 5, tz='UTC')
    assert not BackfillModel.get_overlapping_backfills(o2.dag_id, o2.start_date, o2.end_date, loopback_date, db_session)


def test_get_submitted_backfills(db_session):

    curr_dt = timezone.utcnow()

    # no data
    assert not BackfillModel.get_submitted_backfills(10, db_session)

    # add submitted backfills
    for _ in range(0, 3):
        o = get_random_backfill()
        o.state = BackfillState.SUBMITTED
        db_session.add(o)

    # add a non-submitted backfill
    o = get_random_backfill()
    o.state = BackfillState.QUEUED
    db_session.add(o)

    # add a future scheduled backfill
    o = get_random_backfill()
    o.state = BackfillState.SUBMITTED
    o.scheduled_date = curr_dt + timedelta(days=10)
    db_session.add(o)

    # add a deleted backfill
    o = get_random_backfill()
    o.state = BackfillState.SUBMITTED
    o.deleted = True
    db_session.add(o)

    # case: submitted, limit not hit
    r = BackfillModel.get_submitted_backfills(10, db_session)
    assert len(r) == 3

    # case: submitted, limit hits
    r = BackfillModel.get_submitted_backfills(2, db_session)
    assert len(r) == 2


def test_get_active_backfills(db_session):

    def add_backfills(state, deleted, k=1):
        backfills = []
        for _ in range(0, k):
            o = get_random_backfill()
            o.state = state
            o.deleted = deleted
            db_session.add(o)

            backfills.append(o)
        return backfills

    # no data
    assert not BackfillModel.get_active_backfills(session=db_session)

    # add 2 submitted backfills
    submitted_backfills = add_backfills(BackfillState.SUBMITTED, False, 2)

    # add 3 queued backfills
    queued_backfills = add_backfills(BackfillState.QUEUED, False, 3)

    # add 5 success backfills
    success_backfills = add_backfills(BackfillState.SUCCESS, False, 5)

    # add 1 deleted backfill
    deleted_backfills = add_backfills(BackfillState.SUCCESS, True, 1)

    # case: no filter
    r = BackfillModel.get_active_backfills(session=db_session)
    assert len(r) == 10

    # case: filter by state_list
    r = BackfillModel.get_active_backfills(state_list=[BackfillState.SUBMITTED], session=db_session)
    assert len(r) == 2

    r = BackfillModel.get_active_backfills(state_list=[BackfillState.SUBMITTED, BackfillState.QUEUED], session=db_session)
    assert len(r) == 5

    r = BackfillModel.get_active_backfills(state_list=[BackfillState.FAILED], session=db_session)
    assert len(r) == 0

    r = BackfillModel.get_active_backfills(state_list=[BackfillState.SUBMITTED, BackfillState.FAILED], session=db_session)
    assert len(r) == 2

    # case: filter by dag_ids
    dag_ids = [
        submitted_backfills[0].backfill_dag_id,
        queued_backfills[0].backfill_dag_id,
        success_backfills[0].backfill_dag_id,
        deleted_backfills[0].backfill_dag_id,
        'non-existing-id',
    ]
    r = BackfillModel.get_active_backfills(backfill_dag_ids=dag_ids, session=db_session)
    assert len(r) == 3


def test_get_deletable_backfills(db_session):

    curr_dt = timezone.utcnow()

    # no data
    assert not BackfillModel.get_deletable_backfills(session=db_session)

    # add non-deletable backfill
    o1 = get_random_backfill()
    db_session.add(o1)

    # add deletable backfill
    o2 = get_random_backfill()
    o2.delete_date = curr_dt - timedelta(hours=1)
    db_session.add(o2)

    # add deleted backfill
    o3 = get_random_backfill()
    o3.deleted = True
    db_session.add(o3)

    r = BackfillModel.get_deletable_backfills(session=db_session)
    assert len(r) == 1
    assert r[0].backfill_dag_id == o2.backfill_dag_id
    assert r[0].state == o2.state


def test_queue(db_session):

    # add submitted backfill
    o1 = get_random_backfill()
    o1.state = BackfillState.SUBMITTED
    db_session.add(o1)

    # case: dag_id not found, no updating
    o2 = get_random_backfill()
    o2.backfill_dag_id = 'non-existing-id'
    BackfillModel.queue(o2.backfill_dag_id, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert r.state == BackfillState.SUBMITTED

    # case: update
    o2 = get_random_backfill()
    o2.backfill_dag_id = o1.backfill_dag_id
    BackfillModel.queue(o2.backfill_dag_id, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert r.state == BackfillState.QUEUED


def test_update_run_state(db_session):

    def assert_backfill_equal(o1, o2):
        assert o2.state == o1.state
        assert o2.run_start_date == o1.run_start_date
        assert o2.run_end_date == o1.run_end_date

    curr_dt = timezone.utcnow()
    timestamp_signature = curr_dt - timedelta(hours=1)

    # add a backfill
    o1 = get_random_backfill()
    o1.state = BackfillState.QUEUED
    o1.updated_at = timestamp_signature
    db_session.add(o1)

    # case: id not found, no update
    o2 = get_random_backfill()
    o2.backfill_dag_id = 'non-existing-id'
    o2.state = BackfillState.SUCCESS
    o2.updated_at = timestamp_signature
    BackfillModel.update_run_state(o2.backfill_dag_id, o2.updated_at, o2.state, o2.run_start_date, o2.run_end_date, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert_backfill_equal(r, o1)

    # case: timestamp_signature not match
    o2 = get_random_backfill()
    o2.backfill_dag_id = o1.backfill_dag_id
    o2.state = BackfillState.SUCCESS
    BackfillModel.update_run_state(o2.backfill_dag_id, o2.updated_at, o2.state, o2.run_start_date, o2.run_end_date, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert_backfill_equal(r, o1)

    # case: update
    curr_dt = timezone.utcnow()

    o2 = get_random_backfill()
    o2.backfill_dag_id = o1.backfill_dag_id
    o2.state = BackfillState.SUCCESS
    o2.updated_at = timestamp_signature
    BackfillModel.update_run_state(o2.backfill_dag_id, o2.updated_at, o2.state, o2.run_start_date, o2.run_end_date, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert_backfill_equal(r, o2)
    assert r.updated_at >= curr_dt


def test_update_delete_date(db_session):

    curr_dt = timezone.utcnow()

    # add 2 backfills
    o1 = get_random_backfill()
    o1.state = BackfillState.SUCCESS
    o1.delete_date = curr_dt + timedelta(days=30)
    db_session.add(o1)

    o2 = get_random_backfill()
    o2.state = BackfillState.FAILED
    o2.delete_date = curr_dt + timedelta(days=10)
    db_session.add(o2)

    # def update_delete_date(cls, backfill_dag_ids, delete_date = None, session: Session = None):
    # case: dag id not found, no updating
    delete_date = timezone.utcnow()
    BackfillModel.update_delete_date(['non-existing_id'], delete_date, curr_dt, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert r.delete_date == o1.delete_date
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o2.backfill_dag_id).one()
    assert r.delete_date == o2.delete_date
    assert not db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == 'non-existing_id').one_or_none()

    # case: update one
    delete_date = timezone.utcnow()
    BackfillModel.update_delete_date([o1.backfill_dag_id], delete_date, curr_dt, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert r.delete_date == delete_date
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o2.backfill_dag_id).one()
    assert r.delete_date == o2.delete_date

    # case: update both
    delete_date = timezone.utcnow()
    BackfillModel.update_delete_date([o1.backfill_dag_id, o2.backfill_dag_id], delete_date, curr_dt, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert r.delete_date == delete_date
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o2.backfill_dag_id).one()
    assert r.delete_date == delete_date


def test_delete_dag(db_session):

    # add a backfill
    o1 = get_random_backfill()
    o1.deleted = False
    db_session.add(o1)

    # case: dag id not found, no udpate
    BackfillModel.delete_dag('non-existing_id', db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert r.deleted == o1.deleted
    assert r.updated_at == o1.updated_at

    # case: update
    curr_dt = timezone.utcnow()
    BackfillModel.delete_dag(o1.backfill_dag_id, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert r.deleted
    assert r.updated_at >= curr_dt


def test_delete_submitted_backfills(db_session):

    curr_dt = timezone.utcnow()
    deadline_date = curr_dt + timedelta(minutes=3)

    # add a submitted backfill, scheduled to future
    o1 = get_random_backfill()
    o1.state = BackfillState.SUBMITTED
    o1.scheduled_date = curr_dt + timedelta(hours=2)
    db_session.add(o1)

    # add a submitted backfill, being scheduled
    o2 = get_random_backfill()
    o2.state = BackfillState.SUBMITTED
    o2.scheduled_date = curr_dt
    db_session.add(o2)

    # add a non-submitted backfill
    o3 = get_random_backfill()
    o3.state = BackfillState.QUEUED
    db_session.add(o3)

    backfill_dag_ids = [o1.backfill_dag_id, o2.backfill_dag_id, o3.backfill_dag_id]

    # case: empty ids
    BackfillModel.delete_submitted_backfills([], deadline_date, curr_dt, db_session)
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert not r.deleted
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o2.backfill_dag_id).one()
    assert not r.deleted

    # case: update
    BackfillModel.delete_submitted_backfills(backfill_dag_ids, deadline_date, curr_dt, db_session)
    # o1 is updated
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o1.backfill_dag_id).one()
    assert r.deleted
    # o2 is not due to being scheduled
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o2.backfill_dag_id).one()
    assert not r.deleted
    # o3 is not due to state
    r = db_session.query(BackfillModel).filter(BackfillModel.backfill_dag_id == o3.backfill_dag_id).one()
    assert not r.deleted


def test_list_backfills(db_session):
    column_names = ['backfill_dag_id', 'dag_id', 'deleted']

    # add backfills
    o1 = get_random_backfill()
    db_session.add(o1)
    o2 = get_random_backfill()
    db_session.add(o2)
    o3 = get_random_backfill()
    o3.deleted = True
    db_session.add(o3)

    # 1. no filter
    r = BackfillModel.list_backfills(column_names=column_names, session=db_session)
    assert len(r) == 2

    # 2. dag_id filter
    r = BackfillModel.list_backfills(column_names=column_names, dag_id=o1.dag_id, session=db_session)
    assert len(r) == 1
    assert r[0].dag_id == o1.dag_id

    # 3. include_deleted filter
    r = BackfillModel.list_backfills(column_names=column_names, include_deleted=True, session=db_session)
    assert len(r) == 3


def test_get_backfill_by_id(db_session):
    column_names = ['backfill_dag_id', 'dag_id']

    # add backfills
    o1 = get_random_backfill()
    db_session.add(o1)
    o2 = get_random_backfill()
    db_session.add(o2)

    # 1. valid id
    r = BackfillModel.get_backfill_by_id(column_names, o1.backfill_dag_id, db_session)
    assert r is not None
    assert r.dag_id == o1.dag_id

    # 2. id not exist
    r = BackfillModel.get_backfill_by_id(column_names, "this is an non existing id", db_session)
    assert r is None


def test_add_backfill(db_session):
    column_names = ['backfill_dag_id', 'dag_id']

    o1 = get_random_backfill()
    db_session.add(o1)
    o2 = get_random_backfill()

    # not existing before add
    r = BackfillModel.get_backfill_by_id(column_names, o2.backfill_dag_id, db_session)
    assert r is None

    # exists after add
    BackfillModel.add_backfill(o2, db_session)
    r = BackfillModel.get_backfill_by_id(column_names, o2.backfill_dag_id, db_session)
    assert r is not None
    assert r.dag_id == o2.dag_id
