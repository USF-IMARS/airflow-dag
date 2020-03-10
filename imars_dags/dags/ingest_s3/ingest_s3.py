"""
Ingests sentinel 3 imagery using the sentinelsat package
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


this_dag = DAG(
    dag_id="ingest_s3",
    default_args={
        "start_date": datetime(2018, 11, 20)
    },
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
)
this_dag.doc_md = __doc__

BashOperator(
    task_id=(
        "ingest_s3"
    ),
    bash_command="ingest_s3.sh",
    dag=this_dag
)
