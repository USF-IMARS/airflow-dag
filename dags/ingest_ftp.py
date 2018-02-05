"""
DAG to define the FTP ingest process.

Files are uploaded to the central IMaRS FTP server then this runs and sorts out
where things should go.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from imars_dags.util.globals import DEFAULT_ARGS

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2018, 1, 24, 23, 0),
    'retries': 1
})

this_dag = DAG(
    dag_id="ingest_ftp",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

BashOperator(
    cmd="find /ingest/loc | satfilename ingest",
)

# TODO: for each file in ingest dir
# TODO: check file prefix for known keys (wv2)
# TODO: move file to permanent location
# TODO: (optional) trigger processing DAG for this ingest?
# TODO: (optional) register ingested file in a metadata db?
