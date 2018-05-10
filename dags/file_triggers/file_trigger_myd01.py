from imars_dags.dags.file_triggers.FileTriggerDAG import FileTriggerDAG

this_dag = FileTriggerDAG(
    product_ids=[5],
    dags_to_trigger=[
        "modis_aqua_granule"
    ],
    dag_id="file_trigger_myd01"
)
