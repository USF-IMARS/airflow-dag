# =========================================================================
# wv2 zip from ftp ingest (manually requested from Digital Globe)
# =========================================================================
import airflow  # you need this here or else airflow will not find your dag
from imars_dags.dag_classes.file_triggers.FileTriggerDAG import FileTriggerDAG

this_dag = FileTriggerDAG(
    product_ids=[6],
    dags_to_trigger=[
        "proc_wv2_unzip"
    ],
    dag_id="file_trigger_zip_wv2_ftp_ingest"
)
