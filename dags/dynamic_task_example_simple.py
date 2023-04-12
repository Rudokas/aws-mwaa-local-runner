"""
### Toy example DAG showing dynamic task mapping with XComs.

These features are available in Airflow version 2.3+.

Airflow 2.3+ introduced dynamic task mapping to map over runtime conditions. This DAG goes through using XCOMs to map over a set of values.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

with DAG(
    dag_id="simple-dynamic-task-mapping",
    start_date=datetime(2022, 7, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=__doc__,
) as dag:

    # EXAMPLE 2: upstream task is defined using the TaskFlowAPI,
    # downstream task is defined using a traditional operator
    @task
    def list_s3_keys():
        s3_hook = S3Hook()
        files = s3_hook.list_keys(bucket_name="dainiustest", prefix="pln/")
        return [[file] for file in files]

    def plus_10_traditional(x):
        """Add 10 to x."""
        return x + "pimpalas"

    plus_10_task = PythonOperator.partial(
        task_id="plus_10_task",
        python_callable=plus_10_traditional
    ).expand(
        op_args=list_s3_keys()
    )

    plus_10_task