# unused DAG import so airflow can find your dag
import airflow  # noqa F401

# from imars_dags.util.get_dag_id import get_dag_id
# from imars_dags.util.DAGType import DAGType

from imars_dags.dag_classes.file_triggers.FileTriggerDAG import FileTriggerDAG

this_dag = FileTriggerDAG(
    product_ids=[35],
    dags_to_trigger=[
        # get_dag_id(dag_type=DAGType.PROCESSING, dag_name="l2_to_l3_pass"),
        "processing_l2_to_l3_pass"
    ],
    area_names=['gom'],
    # dag_id=get_dag_id(__file__)
    dag_id='file_trigger_myd0_otis_l2'
)
