"""
Sets up a watch for a product file type in the metadata db.
"""
from datetime import datetime
from datetime import timedelta

from airflow.operators.subdag_operator import SubDagOperator

from imars_dags.operators.FileWatcher.FileTriggerSubDAG import get_subdag
from imars_dags.util.get_default_args import get_default_args

DAWN_OF_TIME = datetime(2018, 5, 5, 5, 5)  # any date in past is fine
SCHEDULE_INTERVAL = timedelta(minutes=1)
# SCHEDULE_INTERVAL must be >= POKE_INTERVAL.
# Also NOTE: SCHEDULE_INTERVAL sets the maximum frequency that products
#   can be ingested at 1 per SCHEDULE_INTERVAL.
POKE_INTERVAL = 60  # use higher value for less load on meta server


class FileWatcherOperator(SubDagOperator):
    def __init__(
        self,
        *args,
        task_id,
        product_ids,
        dags_to_trigger,
        area_names=['na'],
        start_date=DAWN_OF_TIME,
        poke_interval=POKE_INTERVAL,
        default_args=get_default_args(
            start_date=DAWN_OF_TIME,
            retries=0,
        ),
        dag,
        **kwargs
    ):
        """
        Parameters
        ----------
        product_ids : int[]
            list of `product_ids` for the product we are watching.
        dags_to_trigger : str[]
            list of DAG names to trigger when we get a new product.
        area_names: str[]
            list of RoIs that we should consider triggering
            example: ['na', 'gom', 'fgbnms']

        """

        super(FileWatcherOperator, self).__init__(
            *args,
            dag=dag,
            start_date=start_date,
            default_args=default_args,
            subdag=get_subdag(
                dag.dag_id,
                task_id,
                start_date,
                product_ids,
                dags_to_trigger,
                area_names,
                poke_interval
            ),
            task_id=task_id,
            **kwargs
        )
