# unused DAG import so airflow can find your dag
import airflow  # noqa F401

from imars_dags.dag_classes.file_triggers.FileTriggerDAG import FileTriggerDAG

this_dag = FileTriggerDAG(
    product_ids=[5],
    dags_to_trigger=[
        "proc_myd01_to_myd0_otis_l2"
    ],
    area_names=['gom'],
    dag_id="file_trigger_myd01"
)
