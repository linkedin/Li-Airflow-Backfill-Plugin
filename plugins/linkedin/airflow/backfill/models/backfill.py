# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

# to fix definition exists error caused by module repeatly loading
# https://stackoverflow.com/questions/53486813/sqlalchemy-exc-invalidrequesterror-table-already-defined
if __name__ == 'linkedin.airflow.backfill.models.backfill':
    from sqlalchemy import Column, String, Boolean, Enum, Index, Integer, Text, VARCHAR, and_
    from sqlalchemy.sql.expression import false

    from airflow import settings
    from airflow.utils import timezone
    from airflow.models.base import ID_LEN, Base
    from airflow.utils.sqlalchemy import UtcDateTime
    from airflow.utils.log.logging_mixin import LoggingMixin
    from airflow.utils.session import provide_session, NEW_SESSION

    from linkedin.airflow.backfill.utils.backfill_state import BackfillState

    import logging

    def init_models():
        logging.info("init backfill models")

        # create table if not exist
        BackfillModel.__table__.create(settings.engine, checkfirst=True)

    class BackfillModel(Base, LoggingMixin):
        """
        Table containing backfill properties
        """

        __tablename__ = "custom_backfill"    # add custom prefix for non origin airflow tables

        # backfill dag id
        backfill_dag_id = Column(String(ID_LEN), primary_key=True)

        # id of the dag to be backfilled
        dag_id = Column(String(ID_LEN), nullable=False)
        # state
        state = Column(Enum(BackfillState), nullable=False)
        # user description of the backfill
        description = Column(VARCHAR(1000))
        # backfill start date
        start_date = Column(UtcDateTime, nullable=False)
        # backfill end date
        end_date = Column(UtcDateTime, nullable=False)
        # logical dates included in the range
        logical_dates = Column(Text)
        # dag params
        dag_params = Column(Text)
        # max active runs
        max_active_runs = Column(Integer, nullable=False)
        # backfill scheduled date
        scheduled_date = Column(UtcDateTime)
        # delete date
        delete_date = Column(UtcDateTime)
        # origin dag file relative location
        relative_fileloc = Column(VARCHAR(1000))
        # origin code snapshot
        origin_code = Column(Text)
        # submit date
        submitted_at = Column(UtcDateTime)
        # submit user
        submitted_by = Column(VARCHAR(50))
        # update date
        updated_at = Column(UtcDateTime)
        # deleted
        deleted = Column(Boolean, default=False)
        # backfill run start date
        run_start_date = Column(UtcDateTime)
        # backfill run end date
        run_end_date = Column(UtcDateTime)

        __table_args__ = (
            Index('idx_dag_id', dag_id, unique=False),
            Index('idx_state', state, unique=False),
            Index('idx_deleted', deleted, unique=False)
        )

        @classmethod
        @provide_session
        def get_backfills(cls, backfill_dag_ids, column_names, session=None):
            """
            return backfills with columns
            """
            if not backfill_dag_ids:
                return []

            columns = [getattr(cls, name) for name in column_names]
            return session.query(*columns).filter(
                cls.backfill_dag_id.in_(backfill_dag_ids),
            ).all()

        @classmethod
        @provide_session
        def get_backfill_code(cls, backfill_dag_id, session=None):
            """
            return backfill code
            """
            if not backfill_dag_id:
                return None

            return session.query(
                cls.dag_id,
                cls.origin_code,
                cls.submitted_at,
            ).filter(
                cls.backfill_dag_id == backfill_dag_id,
            ).one_or_none()

        @classmethod
        @provide_session
        def get_overlapping_backfills(cls, dag_id, start_date, end_date, loopback_date, session=None):
            """
            get overlapping backfills according to start_date and end_date
            """
            curr_dt = timezone.utcnow()
            return session.query(
                cls.backfill_dag_id,
                cls.state,
                cls.start_date,
                cls.end_date,
            ).filter(
                cls.dag_id == dag_id,
                cls.submitted_at >= loopback_date,
                # not deleted or deleting
                cls.deleted == false(),
                cls.delete_date > curr_dt,
                # overlap check
                and_(cls.start_date <= end_date, cls.end_date >= start_date)
            ).all()

        @classmethod
        @provide_session
        def get_submitted_backfills(cls, limit, session=None):
            """
            return submitted backfill, order by submit date
            """
            curr_dt = timezone.utcnow()
            return session.query(
                cls.backfill_dag_id,
                cls.dag_id,
                cls.start_date,
                cls.end_date,
                cls.logical_dates,
                cls.dag_params,
                cls.max_active_runs,
                cls.relative_fileloc,
                cls.origin_code,
                cls.state,
                cls.updated_at,
            ).filter(
                cls.state == BackfillState.SUBMITTED,
                cls.scheduled_date <= curr_dt,
                cls.deleted == false(),
            ).order_by(cls.submitted_at).limit(limit).all()

        @classmethod
        @provide_session
        def get_active_backfills(cls, state_list=None, backfill_dag_ids=None, session=None):
            """
            return active backfills, order by submit date
            """
            query = session.query(
                cls.backfill_dag_id,
                cls.dag_id,
                cls.state,
                cls.start_date,
                cls.end_date,
                cls.logical_dates,
                cls.updated_at,
            ).filter(
                cls.deleted == false(),
            )
            if state_list:
                query = query.filter(cls.state.in_(state_list))
            if backfill_dag_ids:
                if isinstance(backfill_dag_ids, str):
                    query = query.filter(cls.backfill_dag_id == backfill_dag_ids)
                else:
                    query = query.filter(cls.backfill_dag_id.in_(backfill_dag_ids))
            return query.order_by(cls.submitted_at).all()

        @classmethod
        @provide_session
        def get_deletable_backfills(cls, session=None):
            """
            return deletable backfills, order by delete date
            """
            curr_dt = timezone.utcnow()
            return session.query(
                cls.backfill_dag_id,
                cls.state,
            ).filter(
                cls.deleted == false(),
                cls.delete_date <= curr_dt,
            ).order_by(cls.delete_date).all()

        @classmethod
        @provide_session
        def queue(cls, backfill_dag_id, session=None):
            if not backfill_dag_id:
                return

            update_data = {
                cls.state: BackfillState.QUEUED,
                cls.updated_at: timezone.utcnow(),
            }
            session.query(cls).filter(
                cls.backfill_dag_id == backfill_dag_id,
            ).update(update_data)

        @classmethod
        @provide_session
        def update_run_state(cls, backfill_dag_id, timestamp_signature, state, run_start_date, run_end_date, session=None):
            if not backfill_dag_id:
                return

            update_data = {
                cls.state: state,
                cls.updated_at: timezone.utcnow(),
            }
            if run_start_date:
                update_data[cls.run_start_date] = run_start_date
            if run_end_date:
                update_data[cls.run_end_date] = run_end_date
            session.query(cls).filter(
                cls.backfill_dag_id == backfill_dag_id,
                cls.state != state,
                # avoid concurrently update
                cls.updated_at == timestamp_signature,
            ).update(update_data)

        @classmethod
        @provide_session
        def update_delete_date(cls, backfill_dag_ids, delete_date=None, curr_dt=None, session=None):
            if not backfill_dag_ids:
                return

            if isinstance(backfill_dag_ids, str):
                backfill_dag_ids = [backfill_dag_ids]

            if not curr_dt:
                curr_dt = timezone.utcnow()
            if not delete_date:
                delete_date = curr_dt
            update_data = {
                cls.delete_date: delete_date,
                cls.updated_at: curr_dt,
            }
            session.query(cls).filter(
                cls.backfill_dag_id.in_(backfill_dag_ids),
                cls.deleted == false(),
            ).update(update_data)

        @classmethod
        @provide_session
        def delete_dag(cls, backfill_dag_id, session=None):
            if not backfill_dag_id:
                return

            update_data = {
                cls.deleted: True,
                cls.updated_at: timezone.utcnow(),
            }
            session.query(cls).filter(
                cls.backfill_dag_id == backfill_dag_id,
                cls.deleted == false(),
            ).update(update_data)

        @classmethod
        @provide_session
        def delete_submitted_backfills(cls, backfill_dag_ids, deadline_date, curr_dt=None, session=None):
            if not backfill_dag_ids:
                return

            if isinstance(backfill_dag_ids, str):
                backfill_dag_ids = [backfill_dag_ids]

            if not curr_dt:
                curr_dt = timezone.utcnow()

            update_data = {
                cls.deleted: True,
                cls.updated_at: curr_dt,
            }
            session.query(cls).filter(
                cls.backfill_dag_id.in_(backfill_dag_ids),
                # avoid concurrently update
                cls.state == BackfillState.SUBMITTED,
                cls.deleted == false(),
                # only can delete before scheduled date
                cls.scheduled_date > deadline_date,
            ).update(update_data)

        @classmethod
        @provide_session
        def list_backfills(cls, column_names, dag_id=None, include_deleted=False, session=None):
            """
            list backfills for list api
            """
            columns = [getattr(cls, name) for name in column_names]
            query = session.query(*columns)
            if dag_id:
                query = query.filter(cls.dag_id == dag_id)
            if not include_deleted:
                query = query.filter(cls.deleted == false())

            return query.all()

        @classmethod
        @provide_session
        def get_backfill_by_id(cls, column_names, backfill_dag_id, session=None):
            """
            get backfill
            """
            columns = [getattr(cls, name) for name in column_names]
            return session.query(*columns).filter(cls.backfill_dag_id == backfill_dag_id).one_or_none()

        @classmethod
        @provide_session
        def add_backfill(cls, backfill, session=NEW_SESSION):
            """
            get backfill
            """
            session.add(backfill)
            session.commit()
