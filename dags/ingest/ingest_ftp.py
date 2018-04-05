"""
DAG to define the FTP ingest process.

Files are uploaded to the central IMaRS FTP server then this runs and sorts out
where things should go.
"""
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.globals import DEFAULT_ARGS

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2018, 3, 1, 20, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
})

this_dag = DAG(
    dag_id="ingest_ftp",
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    catchup=False,  # NOTE: this & max_active_runs prevents duplicate extractions
    max_active_runs=1
)

# TODO: better to do this with a FileSensor
#   [ref] : https://stackoverflow.com/questions/44325938/airflow-file-sensor-for-sensing-files-on-my-local-drive
wv2_ingest = BashOperator(
    task_id="wv2_ingest",
    dag = this_dag,
    # `--date` is read in from the filename
    # `--product_type_id` is limited to `zip_wv2_ftp_ingest` b/c of `find` limitations
    #       product_type_id of `zip_wv2_ftp_ingest` is `6`
    # `--status` is `to_load` == 3
    #  `--area`  is `UNCUT`   == 5
    bash_command="""
    /opt/imars-etl/imars-etl.py load \
        --product_type_id 6 \
        --json '{"status":3, "area_id":5}'\
        --directory /srv/imars-objects/ftp-ingest
    """
)

# TODO: should trigger wv2_unzip DAG?
