# =========================================================================
# wv2 zip from ftp ingest (manually requested from Digital Globe)
# =========================================================================
from imars_dags.dags.file_triggers.FileTriggerDAG import FileTriggerDAG

this_dag = FileTriggerDAG(
    product_ids=[6],
    dags_to_trigger=[
        "wv2_unzip"
    ],
    dag_id="file_trigger_zip_wv2_ftp_ingest"
)
