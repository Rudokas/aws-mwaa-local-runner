import logging
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.decorators import task
import smart_open


STAGE = "staging"
STREAM = "motor_registry/emea/pln_motor_registry/year={{ dag_run.logical_date.year }}/month={{ dag_run.logical_date.month }}/"  # noqa: E501
BUCKETS = {
    "RAW": f"dainiustest",
    "STAGE": f"dainiustest-stage",
}

with DAG(
        dag_id='parallel_processing-v1',
        schedule=CronTriggerTimetable("0 3 3 1-12 *", timezone="UTC"),
        start_date=datetime(2021, 1, 1),
        dagrun_timeout=timedelta(minutes=60),
        tags=['dainius_test'],
        catchup=False,
) as dag:

    @task
    def list_s3_keys():
        s3_hook = S3Hook()
        files = s3_hook.list_keys(bucket_name=BUCKETS.get("STAGE"), prefix="motor_registry/emea/pln_motor_registry/year=2023/month=4/")
        return [[file] for file in files]


    def process_file(file):
        with smart_open.open(file) as file_stream:
            file_stream.seek(0)

            logging.info(f"Processing {file}")
        return file


    process_files_parallel = PythonOperator.partial(
        task_id="process_files_parallel",
        python_callable=process_file
    ).expand(
        op_args=list_s3_keys()
    )

    process_files_parallel



