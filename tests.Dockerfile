FROM apache/airflow:2.5.3

USER airflow

# install pytest
RUN python -m pip install -U pytest
RUN python -m pip install pytest-mock

ARG PROJECT_DIR=/home/airflow/projects/backfill-plugins
RUN mkdir -p ${PROJECT_DIR}

WORKDIR ${PROJECT_DIR}

ENTRYPOINT ["/home/airflow/.local/bin/pytest"]
