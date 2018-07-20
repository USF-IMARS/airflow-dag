# unused DAG import so airflow can find your dag
import airflow  # noqa F401

from imars_dags.dag_classes.file_triggers.FileTriggerDAG import FileTriggerDAG

this_dag = FileTriggerDAG(
    product_ids=[35],
    dags_to_trigger=[
        # "processing_l2_to_l3_pass"  # deprecated
    ],
    area_names=[],
    # dag_id=get_dag_id(__file__)
    dag_id='file_trigger_myd0_otis_l2'
)
