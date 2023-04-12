import os
import logging
import shutil
from typing import Sequence
from datetime import datetime, timedelta
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.timetables.trigger import CronTriggerTimetable
from airflow import DAG
from io import BytesIO


STAGE = "staging"
STREAM = "motor_registry/emea/pln_motor_registry/year={{ dag_run.logical_date.year }}/month={{ dag_run.logical_date.month }}/"  # noqa: E501
BUCKETS = {
    "RAW": f"dainiustest",
    "STAGE": f"dainiustest-stage",
}


class SplitLargeCSV(BaseOperator):  # pragma: no cover
    template_fields: Sequence[str] = ("s3_source_key", "s3_target_key_prefix")

    def __init__(
            self,
            s3_source_bucket: str,
            s3_source_key: str,
            s3_target_bucket: str,
            s3_target_key_prefix: str,
            local_path: str,  # /tmp/pln
            chunk_size: int = 100000,
            encoidng: str = "utf-8",
            **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.s3_source_bucket = s3_source_bucket
        self.s3_source_key = s3_source_key
        self.s3_target_bucket = s3_target_bucket
        self.s3_target_key_prefix = s3_target_key_prefix
        self.local_path = local_path
        self.encoding = encoidng
        self.chunk_size = chunk_size
        self.split_s3_key_list = []
        self.s3_hook = S3Hook()

    def write_chunk(self, part, _lines, header):
        original_file_name = self.s3_source_key.split("/")[-1]
        file_full_path = self.local_path + "/split/" + original_file_name.replace(".csv", "") + str(part) + '.csv'
        with open(file_full_path, 'w', encoding=self.encoding) as f_out:
            f_out.write(header)
            f_out.writelines(_lines)
            self.upload_local_file_to_s3(file_full_path)

    def execute(self, context):
        os.makedirs(self.local_path, exist_ok=True)
        os.makedirs(self.local_path + "/split", exist_ok=True)
        file = self.s3_hook.download_file(
            bucket_name=self.s3_source_bucket, local_path=self.local_path, key=self.s3_source_key
        )
        with open(file, 'r', encoding=self.encoding) as f:
            count = 0
            header = f.readline()
            lines = []
            for line in f:
                count += 1
                lines.append(line)
                if count % self.chunk_size == 0:
                    self.write_chunk(count // self.chunk_size, lines, header)
                    lines = []
            # write remainder
            if len(lines) > 0:
                self.write_chunk((count // self.chunk_size) + 1, lines, header)
        # self.xcom_push(context, key="split_file_list", value=self.split_file_list)
        logging.info("Start removing local files")
        shutil.rmtree(self.local_path, ignore_errors=True)
        logging.info("Finished removing local files")
        return [[file] for file in self.split_s3_key_list]

    def upload_local_file_to_s3(self, file_path):
        with open(file_path, "rb") as file:
            s3_key = self.s3_target_key_prefix + file_path.split("/")[-1]
            self.split_s3_key_list.append(s3_key)
            file_obj = BytesIO(file.read())
            file_obj.seek(0)
            self.s3_hook.load_file_obj(
                file_obj=file_obj,
                bucket_name=self.s3_target_bucket,
                key=s3_key,
            )
            logging.info(f"File {s3_key} uploaded to s3")


with DAG(
        dag_id='split-csv-file-v1',
        schedule=CronTriggerTimetable("0 3 3 1-12 *", timezone="UTC"),
        start_date=datetime(2021, 1, 1),
        dagrun_timeout=timedelta(minutes=60),
        tags=['dainius_test'],
        catchup=False,
) as dag:
    split = SplitLargeCSV(
        task_id="split",
        s3_source_bucket=BUCKETS["RAW"],
        s3_source_key="pln/2023-03-02_CARVERTICAL_UM-0.csv",
        s3_target_bucket=BUCKETS["STAGE"],
        s3_target_key_prefix=STREAM,
        local_path="/tmp/pln",
        chunk_size=100000,
        encoidng="ISO-8859-1",
    )