# unused DAG import so airflow can find your dag
import airflow  # noqa F401

from imars_dags.dag_classes.file_triggers.FileTriggerDAG import FileTriggerDAG
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.DAGType import DAGType

this_dag = FileTriggerDAG(
    product_ids=[5],
    dags_to_trigger=[
        # "proc_myd01_to_myd0_otis_l2"  # deprecated
        get_dag_id(
            dag_name="modis_aqua_pass", dag_type=DAGType.PROCESSING
        )
    ],
    area_names=['gom', 'fgbnms'],
    dag_id="file_trigger_myd01"
)
