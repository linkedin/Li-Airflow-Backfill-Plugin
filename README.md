# Li-Airflow-Backfill-Plugin
[GitHub Link of this page](https://github.com/linkedin/Li-Airflow-Backfill-Plugin)

## Airflow Backfill Plugin
This is an [Airflow Plugin](https://airflow.apache.org/docs/apache-airflow/2.5.3/authoring-and-scheduling/plugins.html). It provides full-featured UI and APIs for data backfills in Airflow with manageability and scalability.

![](/docs/static/img/backfill2.png)
![](/docs/static/img/backfill4.png)

## Features
We want users to be able run backfills in a scheduled, managed, scalable, and robust way:
- Backfills can be triggered directly with desired options
- Backfills can be monitored and manageable
- Backfills are scalable
- Resilient to Airflow system restarts and failures
- Allow scheduled Dags to run while backfilling
- Proper controlling of resources used by backfills

These features can be easily added to an Airflow instances since it is an Airflow plugin.

## Quick Start
Let's get started by running Airflow and backfill at local docker: (docker is required, [refer to Airflow doc](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) for more details in running Airflow in docker):
```zsh
# in project root folder
# start
docker-compose up
# stop
docker-compose down
```
To access Airflow web: http://localhost:8080 (user/pass airflow:airflow)

## Backfill User Doc
- [Intro](/docs/intro.md)
- [Backfill UI](/docs/backfill-ui.md)
- [Backfill APIs](/docs/backfill-api.md)

## Supported Airflow Version

The supported Airflow version is 2.5.3.

Other versions are exected to work. To quickly test other versions:
- first, make sure 2.5.3 works at your local docker, and backup [docker-compose.yaml in project root folder](/docker-compose.yaml)
- fetch and copy [the docker-compose.yaml from the testing Airflow version](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml) to the project root
- add the volumns values as the following to it: (refering to the docker-compose.yaml backuped above)
```
volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/plugins/linkedin/airflow/backfill:/opt/airflow/plugins/linkedin/airflow/backfill
```
- follow [Quick Start](#quick-start) to run Airflow.

## Deploy Backfill Plugin to an Existing Airflow Instance

### Deploy Plugin
**Option 1: Drop files to Airflow plugins folder**

As [Apache Airflow doc says](https://airflow.apache.org/docs/apache-airflow/2.5.3/authoring-and-scheduling/plugins.html#plugins), simply drop all the content in the [plugins folder in the project root](/plugins) to the $AIRFLOW_HOME/plugins folder of the Airflow instance. 

Restart of the Airflow [may be needed according the Airflow config](https://airflow.apache.org/docs/apache-airflow/2.5.3/authoring-and-scheduling/plugins.html#when-are-plugins-re-loaded) to enable the backfill plugin.

**Option 2: Install from PyPi**

Starting from 1.0.2, Backfill plugin is available in PyPi. After installation, backfill lib will be installed and registered through entry_points in [setup.py](/setup.py).
```shell
pip install li-airflow-backfill-plugin==1.0.2
```

### Deploy System Dags

Some Dags are needed to make backfill work. After enabling backfill plugin, drop all the content in the [dags/backfill_dags folder](/dags/backfill_dags/) to the configured Airflow Dags folder (default is $AIRFLOW_HOME/dags) of the Airflow instance. 

Restart of the Airflow is not needed.

## Plugin Development

**Run Airflow**

After making changes to the source code, you can run the Airflow in local docker as described in [Quick Start](#quick-start) to test. The logs will appear in the logs folder in the project root, and feel free to add testing Dags to the [dags folder](/dags).

**Unit Test**

pytest is used to run unit tests in docker. The test source code is in [tests folder](/tests) and the pytest configure is [pytest.ini](/pytest.ini)

Build Image once for all:
```zsh
# in project root folder
docker build -t airflow-backfill-plugin-tests-1 -f tests.Dockerfile .
```

Run Unit Test:
```zsh
# in project root folder
./run_tests.sh
```

## Advanced Topics

For detailed design, please refer to the [Design Doc](/docs/static/pdf/Li_Airflow_Backfill_Design.pdf).

### Assumptions

**Writing files to Airflow Dags folder**

By default, backfill Dag files will be created in dags/backfill_user_dags folder in workers.
This limitation may be lifted through [backfill store customerization](#backfill-store-customerization).

**Shallow copy**

The backfill Dags are shallow copies from the origin Dags, which means if dependencies outside of the Dag definition file change while backfills are running, the actual behavior may change accordingly.

### Database

A [backfill table](/plugins/linkedin/airflow/backfill/models/backfill.py) is automactically created and leveraged to store backfill meta and status information in the default Airflow database.

No other tables are created or modified by the backfill feature.

### Backfill Store Customerization

**Backfill Dag Id Conventions**

The Backfill Dag Ids are generated by [backfill store](/plugins/linkedin/airflow/backfill/utils/backfill_store.py). By default, the Id will be origin Dag Id affixed with "backfill" and timestamp.

The backfill Dag Id is customizable by setting AIRFLOW__LI_BACKFILL__BACKFILL_STORE env to new store class. For example:
```
name: AIRFLOW__LI_BACKFILL__BACKFILL_STORE
value: 'airflow.providers.my_porvider.backfill.backfill_store.MyBackfillStore'
```

**Backfill Dags Persistence**

The backfill Dag files by default are persisted to dags/backfill_user_dags folder.

The persistence is customizable, for example, to store Dags through APIs.


### Security
**Authetication**

Backfill, both UI and APIs, is integrated into the existing [Airflow authetication model](https://airflow.apache.org/docs/apache-airflow/2.5.3/administration-and-deployment/security/index.html), so they are autheticated as other Airflow UI and APIs. By default and in local docker Airflow instance, username and password are used to autheticate.

**Access Control**

Airflow RBAC is supported through [permission module](/plugins/linkedin/airflow/backfill/security/permissions.py).

- Backfill Menus Access: Backfill menu resources are defined and requires menu access permission to show the menu in the UI
- Backfill Access: Backfill resource can be create (create backfill), read (list backfill), and operate (cancel or delete backfill)
- Backfill Dag Access: Backfill Dags inherit Dag access permissions from the orgin Dags
- Origin Dags to backfill: Must have can_edit permission for the origin Dag to start backfill

By default, all the backfill permissions are automatically granted to "User" and "Op" roles. This can be customerized through AIRFLOW__LI_BACKFILL__PERMITTED_ROLES. For example:
```
name: AIRFLOW__LI_BACKFILL__PERMITTED_ROLES
value: 'first_role,second_role'
```
