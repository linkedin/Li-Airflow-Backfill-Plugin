import os


def test_airflow_home():
    # case airflow home does NOT exist
    del os.environ['AIRFLOW_HOME']
    airflow_home_folder = os.getenv("AIRFLOW_HOME") or "/default_folder"
    assert airflow_home_folder == "/default_folder"
    assert os.path.join(airflow_home_folder, "backfill/dags") == "/default_folder/backfill/dags"

    # case airflow home exists
    os.environ["AIRFLOW_HOME"] = "/opt/airflow"
    airflow_home_folder = os.getenv("AIRFLOW_HOME") or "/default_folder"
    assert airflow_home_folder == "/opt/airflow"
    assert os.path.join(airflow_home_folder, "backfill/dags") == "/opt/airflow/backfill/dags"
