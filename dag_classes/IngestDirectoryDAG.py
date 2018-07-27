"""
DAG that ingests files from a directory into IMaRS metadata db
(and does nothing else... we assume).

Example usage:
--------------
IngestDirectoryDAG(
    directory_to_watch='/srv/imars-objects/ftp-ingest',
    schedule_interval=timedelta(days=1),
    etl_load_args={
        'product_id': 6,
        'status_id': 3,
        'area_id': 5,
        'duplicates_ok': True,
        'nohash': True,
    },
)
"""
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import imars_etl

from imars_dags.util.get_default_args import get_default_args


class IngestDirectoryDAG(DAG):
    def __init__(
        self,
        *args,
        schedule_interval=timedelta(days=1),
        **kwargs
    ):
        """
        parameters:
        -----------

        """
        super(IngestDirectoryDAG, self).__init__(
            *args,
            schedule_interval=schedule_interval,
            catchup=False,
            max_active_runs=1,
            default_args=get_default_args(
                start_date=datetime.utcnow(),
                retries=0,
            ),
            **kwargs
        )

    @staticmethod
    def _do_ingest_task(etl_load_args, **kwargs):
        # default args added to every ingest:
        load_args = dict(
            verbose=3,
            status_id=3,
            # TODO: should these be automaticaly added?
            # duplicates_ok=True,
            # nohash=True,
        )
        # explicitly passed args overwrite defaults above
        load_args.update(etl_load_args)
        imars_etl.load(
            **load_args
        )
        # TODO: check output
        #       Mark skipped if none loaded, else mark success.

    def add_ingest_task(self, task_id, etl_load_args):
        return PythonOperator(
            dag=self,
            task_id=task_id,
            op_args=[
                etl_load_args
            ],
            python_callable=self._do_ingest_task,
        )
