# you need this here or else airflow will not find your dag
import airflow  # noqa F401

from imars_dags.dag_classes.file_triggers.FileTriggerDAG import FileTriggerDAG
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.DAGType import DAGType

# NOTE: proc_wv2_classification also requires the following product:
# | 14 | xml_wv2_m1bs           |
#
# For now we just assume that this 2nd product is available when the 1st
# product (below) is ready, and we mark this 2nd product `loaded` by
# including it in the `catchall_unused` FileTriggerDAG.
# TODO: How to FileTriggerDAG with multiple input products?

# | 11 | ntf_wv2_m1bs           | wv2 1b multispectral .ntf          |
this_dag = FileTriggerDAG(
    product_ids=[11],
    dags_to_trigger=[
        # "proc_wv2_classification"  # deprecated
        get_dag_id(
            dag_name="wv2_classification", dag_type=DAGType.PROCESSING
        )
    ],
    area_names=['na', 'big_bend'],
    dag_id="file_trigger_ntf_wv2_m1bs"
)
