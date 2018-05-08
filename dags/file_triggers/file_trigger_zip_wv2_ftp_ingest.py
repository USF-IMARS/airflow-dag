# =========================================================================
# wv2 unzip to final destination
# =========================================================================
from datetime import datetime,timedelta

from airflow import DAG
from imars_dags.operators.MMTTriggerDagRunOperator import MMTTriggerDagRunOperator

from imars_dags.util.globals import DEFAULT_ARGS
from imars_dags.dags.file_triggers.FileTriggerDAG import FileTriggerDAG

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2018, 3, 5, 16, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
})

this_dag = FileTriggerDAG(
    product_id=6,
    dags_to_trigger=[
        "wv2_unzip"
    ],
    dag_id="file_trigger_zip_wv2_ftp_ingest",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
)
