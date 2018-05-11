import airflow  # you need this here or else airflow will not find your dag

from imars_dags.dags.file_triggers.FileTriggerDAG import FileTriggerDAG

this_dag = FileTriggerDAG(
    product_ids=[5],
    dags_to_trigger=[
        "myd01_to_myd0_otis_l2"
    ],
    dag_id="file_trigger_myd01"
)
