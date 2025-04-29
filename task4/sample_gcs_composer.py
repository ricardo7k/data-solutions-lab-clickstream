import datetime

import airflow
from airflow.operators.bash import BashOperator


with airflow.DAG(
    "composer_sample_trigger_response_dag",
    start_date=datetime.datetime(2021, 1, 1),
    # Not scheduled, trigger only
    schedule_interval=None,
) as dag:
    # Print the dag_run's configuration, which includes information about the
    # Cloud Storage object change.
    print_gcs_info = BashOperator(
        task_id="print_gcs_info", bash_command="echo {{ dag_run.conf }}"
    )