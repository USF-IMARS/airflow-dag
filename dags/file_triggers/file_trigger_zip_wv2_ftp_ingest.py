# =========================================================================
# wv2 unzip to final destination
# =========================================================================
from datetime import datetime,timedelta

from airflow import DAG
from imars_dags.operators.MMTTriggerDagRunOperator import MMTTriggerDagRunOperator

from imars_dags.dags.file_triggers.FileTriggerDAG import FileTriggerDAG

this_dag = FileTriggerDAG(
    product_id=6,
    dags_to_trigger=[
        "wv2_unzip"
    ],
    dag_id="file_trigger_zip_wv2_ftp_ingest"
)
