"""
DAG to define the FTP ingest process.

Files are uploaded to the central IMaRS FTP server then this runs and sorts out
where things should go.
"""
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from imars_dags.util.globals import DEFAULT_ARGS

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2018, 3, 1, 20, 0),
    'retries': 1
})

this_dag = DAG(
    dag_id="ingest_ftp",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

wv2_ingest = BashOperator(
    task_id="wv2_ingest",
    dag = this_dag,
    # `--date` is read in from the filename
    # `--type` is limited to `zip_wv2_ftp_ingest` b/c of `find` limitations
    #       product_type_id of `zip_wv2_ftp_ingest` is `6`
    # `--status` is `to_load` == 3
    bash_command="""
    find /srv/imars-objects/ftp-ingest/wv2_*zip -type f -exec \
    /opt/imars-etl/imars-etl.py load \
        --type 6 \
        --json '{"status":3}'\
        --filepath {} \;
    """
)
