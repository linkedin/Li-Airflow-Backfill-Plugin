PROJECT_DIR=/home/airflow/projects/backfill-plugins
docker run -v ${PWD}/pytest.ini:${PROJECT_DIR}/pytest.ini -v "${PWD}/plugins:${PROJECT_DIR}/plugins" -v "${PWD}/dags:${PROJECT_DIR}/dags" -v "${PWD}/tests:${PROJECT_DIR}/tests"  airflow-backfill-plugin-tests-1
