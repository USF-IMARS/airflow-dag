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
# =========================================================================
# wv2 unzip to final destination
# =========================================================================
# === wait for a valid target to process
check_for_to_loads = SqlSensor(
    conn_id="conn_id",
    sql="SELECT id FROM file WHERE status=3 AND type=6",
    soft_fail=True
)
wv2_ingest >> check_for_to_loads

# === Extract
def extract_file(**kwargs):
    fname = imars_etl.extract(sql=sql_str)
    return fname

extract_file = PythonOperator(
    task_id='extract_file',
    provide_context=True,
    python_callable=extract_file,
    dag=this_dag
)
check_for_to_loads >> extract_file

# === Transform
OUTPUT_FILE = "/tmp/airflow_output_{{ execution_date }}"
unzip_wv2_ingest = BashOperator(
    task_id="unzip_wv2_ingest",
    dag = this_dag,
    bash_command="""
        unzip \
            -i {{ ti.xcom_pull("extract_file.fname") }}
            -o {{ params.OUTPUT_FILE }} \
    """,
    params={
        "OUTPUT_FILE": OUTPUT_FILE
    }
)
extract_file >> unzip_wv2_ingest

# === load result(s)
def load_file(**kwargs):
    metadata={
        "TODO":"fill this"
        "OUTPUT_FILE": OUTPUT_FILE
    }
    imars_etl.load(metadata)

load_file = PythonOperator(
    task_id='load_file',
    provide_context=True,
    python_callable=load_file,
    dag=this_dag
)
unzip_wv2_ingest >> load_file

# === wv2 schedule zip file for deletion
# TODO:
"""UPDATE file SET status="to_delete" WHERE id={record_id}"""
