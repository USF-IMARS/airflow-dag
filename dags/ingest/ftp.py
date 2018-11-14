"""
DAG to define the FTP ingest process.

Files are uploaded to the central IMaRS FTP server then this runs and sorts out
where things should go.
"""
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.get_default_args import get_default_args


# TODO: should use IngestDirectoryDAG here
this_dag = DAG(
    dag_id="ingest_ftp",
    default_args=get_default_args(
        start_date=datetime(2018, 9, 30, 2, 22),
        retries=0,
        retry_delay=timedelta(minutes=3),
    ),
    schedule_interval="2 22 * * 0",
    catchup=False,  # NOTE: this & max_active_runs prevents duplicate ingests
    max_active_runs=1
)

CMD = """
python3 -m imars_etl find /srv/imars-objects/ftp-ingest \
    --product_id __PRODUCT_ID__ \
| xargs -n 1 -i sh -c ' \
    python3 -m imars_etl -v load \
        --product_id __PRODUCT_ID__ \
        {} && \
    rm {}\
'
"""
wv2_ingest = BashOperator(
    task_id="wv2_ingest",
    dag=this_dag,
    bash_command=CMD.replace("__PRODUCT_ID__", 6)
)

wv3_ingest = BashOperator(
    task_id="wv3_ingest",
    dag=this_dag,
    bash_command=CMD.replace("__PRODUCT_ID__", 47)
)

# NOTE: one after the other to reduce stress on ftp-ingest server
#   these could be parallel if disks & network could handle it.
wv2_ingest >> wv3_ingest
