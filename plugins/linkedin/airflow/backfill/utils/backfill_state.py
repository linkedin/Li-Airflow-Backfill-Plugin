# Copyright (c) LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
# See LICENSE in the project root for license information.

import enum


class BackfillState(str, enum.Enum):
    """
    backfill state
    backfill life cycle in state:
    submitted -------------> queued -------------> running / paused -------------> success / failed
                            [          dag run cancellable         ]
    [cancellable before start]
                            [                                  created                             ]
                            [                                  updatable                           ]
                                                                                [     finished     ]
    """
    SUBMITTED = "submitted"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    SUCCESS = "success"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def is_cancelable(cls, state):
        """
        if ok to cancel backfill Dag runs
        """
        return state in [
            BackfillState.PAUSED,
            BackfillState.QUEUED,
            BackfillState.RUNNING,
        ]

    @classmethod
    def is_updatable(cls, state):
        """
         if ok to update state for a backfill, backfill Dag must be created
        """
        return state != BackfillState.SUBMITTED

    @classmethod
    def is_deletable(cls, state):
        return state in [
            BackfillState.PAUSED,
            BackfillState.SUCCESS,
            BackfillState.FAILED,
        ]

    @classmethod
    def is_finished(cls, state):
        """
        finish states for a backfill
        """
        return state in [
            BackfillState.SUCCESS,
            BackfillState.FAILED,
        ]

    @classmethod
    def is_created(cls, state):
        """
        if a backfill Dag was created
        """
        return state != BackfillState.SUBMITTED

    @classmethod
    def get_backlog_states(cls):
        return [
            BackfillState.PAUSED,
            BackfillState.SUCCESS,
            BackfillState.FAILED,
        ]

    @classmethod
    def get_updating_states(cls):
        return [
            BackfillState.QUEUED,
            BackfillState.RUNNING,
        ]
