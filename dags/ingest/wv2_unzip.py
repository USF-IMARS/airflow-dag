# =========================================================================
# wv2 unzip to final destination
# =========================================================================
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.sensors import SqlSensor

from imars_dags.util.globals import DEFAULT_ARGS

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2018, 3, 5, 16, 0),
    'retries': 1
})

this_dag = DAG(
    dag_id="wv2_unzip",
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# === wait for a valid target to process
SQL_STR="SELECT id FROM file WHERE status=3 AND type=6"
check_for_to_loads = SqlSensor(
    task_id='check_for_to_loads',
    conn_id="imars_metadata",
    sql=SQL_STR,
    soft_fail=True,
    dag=this_dag
)

# TODO: should set imars_product_metadata.status to "processing" to prevent
#    duplicates?

# === Extract
def extract_file(**kwargs):
    fname = imars_etl.extract(sql=SQL_STR)
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
# TODO: verify xcom usage [ref]: https://groups.google.com/forum/#!topic/airbnb_airflow/ZEdpZbxawNc
unzip_wv2_ingest = BashOperator(
    task_id="unzip_wv2_ingest",
    dag = this_dag,
    bash_command="""
        unzip \
            {{ ti.xcom_pull("extract_file.fname") }} \
            -d {{ params.OUTPUT_FILE }}
    """,
    params={
        'OUTPUT_FILE': OUTPUT_FILE,
    }
)
extract_file >> unzip_wv2_ingest

# === load result(s)
def load_file(**kwargs):
    metadata={
        "TODO":"fill this",
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
update_input_file_meta_db = MySqlOperator(
    task_id="update_input_file_meta_db",
    sql="""UPDATE file SET status="to_delete" WHERE id={record_id}""",
    mysql_conn_id='imars_metadata_database',  # TODO: setup imars_metadata connection
    autocommit=False,  # TODO: True?
    parameters=None,
    dag=this_dag
)
load_file >> update_input_file_meta_db
