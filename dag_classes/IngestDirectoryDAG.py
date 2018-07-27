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

from imars_dags.util.sanitize_to_py_var_name import sanitize_to_py_var_name
from imars_dags.util.DAGType import DAGType
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.get_default_args import get_default_args


class IngestDirectoryDAG(DAG):
    def __init__(
        self,
        directory_to_watch,
        *args,
        schedule_interval=timedelta(days=1),
        etl_load_args={},
        **kwargs
    ):
        """
        parameters:
        -----------
        directory_to_watch : local filepath
            the directory we want to load files from
        etl_load_args : dict
            arguments to pass through to the imars_etl.load function
        """
        self.directory_to_watch = directory_to_watch
        self.etl_load_args = etl_load_args
        self.dag_id = get_dag_id(
            dag_type=DAGType.INGEST,
            dag_name=sanitize_to_py_var_name(self.directory_to_watch).lower()
        )
        super(IngestDirectoryDAG, self).__init__(
            dag_id=self.dag_id,
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
        self._add_ingest_task()

    @staticmethod
    def _do_ingest_task(directory_to_watch, etl_load_args, **kwargs):
        imars_etl.load(
            # TODO: should these be automaticaly added?
            # status_id=3,
            # duplicates_ok=True,
            # nohash=True,
            verbose=3,

            directory=directory_to_watch,
            **etl_load_args
        )
        # TODO: check output
        #       Mark skipped if none loaded, else mark success.

    def _add_ingest_task(self):
        return PythonOperator(
            task_id=('imars_etl_load'),
            dag=self,
            op_args=[
                self.directory_to_watch,
                self.etl_load_args
            ],
            python_callable=self._do_ingest_task,
        )
