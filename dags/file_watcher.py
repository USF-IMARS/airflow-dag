# =========================================================================
# this sets up a FileTriggerDAG that catches a whole bunch of products
# which do not launch DAGs and changes their status from `to_load` to `std`
# =========================================================================
# unused DAG import so airflow can find your dag
from airflow import DAG

from imars_dags.operators.FileWatcher.FileWatcherOperator \
    import FileWatcherOperator


this_dag = DAG(
    dag_id="file_watcher",
    catchup=False,  # latest only
)
with this_dag as dag:
    unprocessed_watcher_task = FileWatcherOperator(
        task_id="unprocessed_watcher_task",
        product_ids=[x for x in range(7, 35) if x not in [11]],
        dags_to_trigger=[],
    )
    file_trigger_myd0_otis_l2 = FileWatcherOperator(
        task_id='file_trigger_myd0_otis_l2',
        product_ids=[35],
        dags_to_trigger=[
            # "processing_l2_to_l3_pass"  # deprecated
        ],
        area_names=[],
    )
