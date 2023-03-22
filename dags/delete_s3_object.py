from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator


STAGE = "staging"
STREAM = "motor_registry/emea/fra_motor_registry/year={{ dag_run.logical_date.year }}/month={{ dag_run.logical_date.month }}"  # noqa: E501
BUCKETS = {
    "RAW": f"persistent-resources-{STAGE}-data-lake-raw",
    "STAGE": f"persistent-resources-{STAGE}-data-lake-stage",
}


with DAG(
    dag_id='delete-s3-object',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['s3_test'],
    catchup=False,
) as dag:
    delete_zip_file = S3DeleteObjectsOperator(
        task_id="delete_zip_file",
        bucket=BUCKETS["STAGE"],
        keys=f"{STREAM}/data_{{{{ execution_date.strftime('%Y%m') }}}}.zip",
    )
