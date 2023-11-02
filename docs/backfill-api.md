# Backfill APIs

## API Endpoint

Backfill API base path: **/api/v1/plugins/backfills**

## API Interfaces

### List backfills: GET /

List backfills optionally with filters.

RBAC: only list backfill dags with can_read

Params:

-   dag_id: string, optional, filter by origin dag_id
-   start_date: datetime string, optional, filter by a time range that
    contains the logical dates of a backfill
-   end_date: datetime string, must provide if start_date is provided
-   include_deleted: boolean, default false

Response: 200 success, others fail with exception

Return: a list of [backfill object](#backfill-object)

Example API call through curl:

```{=html}
curl "http://${SERVER}:${PORT}/api/v1/plugins/backfills?include_deleted=1&start_date=2023-01-01T08%3A00%3A00%2B00%3A00&end_date=2023-01-01T08%3A00%3A00%2B00%3A00" -H 'Content-Type: application/json'"
```
### Create backfill: POST /

Create a backfill from an origin dag.

RBAC: must have can_edit to origin dag

Request body:

-   dag_id: string, required
-   description: string, optional
-   start_date: datetime string, required, range start for logical dates
-   end_date: datetime string, required, range end for logical dates
-   additional_dag_params: JSON object, optional
-   scheduled_at: datetime string, default to now
-   max_active_runs: int, default to 1
-   delete_after_days: int, default to 30
-   ignore_overlapping_backfills: boolean, default to false

Response: 200 success, others fail with exception

Return: [backfill object](#backfill-object)

Example API call through curl:

```{=html}
curl -X POST "http://${SERVER}:${PORT}/api/v1/plugins/backfills" -H 'Content-Type: application/json' -d '{"dag_id": "test_data_sensor", "description": "test00", "start_date": "2023-01-01", "end_date": "2023-01-02", "ignore_overlapping_backfills": "false", "dag_params": "{}", "scheduled_date": "2023-01-01", "max_active_runs": 1, "delete_duration_days": 30}'
```
### Get backfill: GET /

Get backfill metadata.

RBAC: must have can_read to backfill dag

Path params: backfill_dag_id

Response: 200 success, others fail

Return: [backfill object](#backfill-object)

Example API call through curl:

```{=html}
curl "http://${SERVER}:${PORT}/api/v1/plugins/backfills/test_long_running_worker_backfill_20230622_230245" -H 'Content-Type: application/json'
```
### Cancel backfill runs: POST /cancel

Cancel backfill runs.

RBAC: must have can_operate to backfill dag

Path params: backfill_dag_id

Response: 200 success, others fail with exception

Return: [backfill object](#backfill-object)

Example API call through curl:

```{=html}
curl -X POST "http://${SERVER}:${PORT}/api/v1/plugins/backfills/test_data_sensor_backfill_20230622_234238/cancel" -H 'Content-Type: application/json'
```
### Delete backfill: DELETE /

Delete backfill Dag. (async deletion later)

RBAC: must have can_operator to backfill dag

Path params: backfill_dag_id

Response: 200 success, others fail with exception

Return: [backfill object](#backfill-object)

Example API call through curl:

```{=html}
curl -X DELETE "http://${SERVER}:${PORT}/api/v1/plugins/backfills/test_data_sensor_backfill_20230622_234238" -H 'Content-Type: application/json'
```
## Backfill Object

This is the object to represent backfill

-   state: enum string, submitted, queued, running, paused, success,
    failed
-   backfill_dag_id: string
-   description: string, backfill description
-   dag_id: string, original dag id to be backfilled
-   start_date: datetime string, range start of the logical dates
-   end_date: datetime string, range end of the logical dates
-   logical_dates: list of datetime string
-   run_start_date: datetime string, backfill run start datetime
-   run_end_date: datetime string, backfill run end datetime
-   dag_params: JSON object, dag key value params
-   max_active_runs: int, 1-3, max active runs of the backfill dag
-   submitted_by: string, idap of submit user
-   submitted_at: datetime string, submit datetime
-   scheduled_at: datetime string, backfill scheduled datetime
-   updated_at: datetime string, update datetime
-   deleted: string, Deleted, ToBeDeleted, and empty for non-deleted
