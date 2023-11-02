# Backfill UI

## How to start a backfill for your dag?

You can create a backfill through the Backfill List page under the
Backfill main menu. To access this page, click on the *List Backfills*
dropdown menu located in the Backfill main menu at the top of the
Airflow UI.

This action will display the backfill management page. From there, click
on the *+* button to access the backfill creation page (located on the
right side below).

![](/docs/static/img/backfill1.png)

**Attention:** If the menus or add button are not visible, it indicates
that you do not have the necessary authorization. Please contact Admin to grant you the appropriate role.

If you are not able to see the Dag for which you want to trigger a
backfill, please contact the Dag owner to grant you permission.

![](/docs/static/img/backfill2.png)

-   Select Dag: Choose the specific Dag that you want to backfill.
-   Description: Provide a description for reference purposes, which
    can be used for future searches.
-   Start Date: Specify the starting date and time for the backfill
    process.
-   End Date: Specify the ending date and time for the backfill
    process.
-   Additional Dag Params: Add any extra parameters to supplement
    the existing Dag parameters, if applicable.
-   Schedule Backfill to Run at: Decide whether to schedule the
    backfill for a future time or execute it immediately for the current
    time.
-   Max Concurrent Dag Runs: Determine the maximum number of
    concurrent logical dates to run during the backfill process.
-   Delete Created Backfill Dag After: Set the number of days after
    which the backfill Dag should be automatically deleted. Note that
    the backfill metadata will not be deleted, and users can manually
    delete the Dag from the List Page in the management section.
-   Ignoring Overlapping Backfills: Enable this option if you want
    to force the backfill even if the specified date range (Start Date
    to End Date) overlaps with existing backfills of the same Dag.
    Please exercise caution when requesting concurrent backfills of the
    same Dag, as conflicts among the Dag runs themselves may arise.

Please be mindful of the potential conflicts that can occur with
concurrent backfills of the same Dag. Click the "Submit" button to
create a backfill. You will see the successful message.

![](/docs/static/img/backfill2_1.png)

When a backfill is initiated, a Dag
specifically designed for the backfill process will be created
automatically. Once the Dag is created, it will be started and begin
executing the necessary tasks according to the specified date range or
criteria defined for the backfill.

## Monitoring of Backfills

Once a backfill is requested, a Backfill Dag will be created and
visible in Dag UI in a minute as a regular Dag. You can check the
status, log, graph, etc for it.

![](/docs/static/img/backfill3.png)

**Please be aware:**

-   Code tab does not show the actual Dag code, please visit Backfill
    Management page for the origin Dag code.
-   Like other Dags, Backfill Dag runs could be changed like stopped,
    cancelled, resumed etc. Please be cautious about those operations
    since they may break the backfill state update.

## Backfills Management

Please click on "List Backfills" dropdown menu under Backfill main menu
at the top of Airflow UI to visit backfill management page:

![](/docs/static/img/backfill4.png)

Management Page shows all backfills for which a User has access.
Backfills can be filtered through "search" panel and actions can be
performed accordingly.

Dag metadata is shown in the list by columns. Here is the explanation
for the some of the columns:

-   Deleted: if the Backfill Dag is deleted or marked as deleted.
-   State: State of the backfill. State is updated periodically, may not
    be up-to-date, please refer to Backfill Dag Page for all the
    details.
-   Origin Dag: the original dag which is being backfilled. The
    "Snapshot" link shows the code of the origin Dag at the time of
    backfill creation. The origin dag changes made after backfill
    creation won't be applied to backfill.
-   Delete Date: the date when backfill dag will be deleted.

Users can perform some actions to manage backfills. Here are the
possible actions:

-   Cancel Backfill: Stop the selected Backfills' Dag runs and
    pause the Backfill Dag to prevent scheduling remaining runs
-   Delete Backfill Dag: Mark the selected backfill Dags to be deleted
    automatically in a few hours.
