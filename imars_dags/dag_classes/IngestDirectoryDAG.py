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
import shutil

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import imars_etl

from imars_dags.util.merge_dicts import merge_dicts


class IngestDirectoryDAG(DAG):
    # these COMMON_ARGS get added to every imars-etl call,
    # but can be overridden in the load_kwargs_list passed in.
    COMMON_ARGS = {
        'duplicates_ok': True,  # don't freak out over duplicates
        'nohash': True,  # speeds things up a lot if True
        'status_id': 3,
        # 'dry_run': True,  # True if we are just testing
    }

    def __init__(
        self,
        *args,
        directory_path,
        load_kwargs_list,
        rm_loaded=False,
        schedule_interval=timedelta(days=1),
        **kwargs
    ):
        """
        parameters:
        -----------
        directory_path : str path
            path we are loading
        load_kwargs_list : list of dict
            kwargs to pass to the load operation for each load
        rm_loaded : bool
            Loaded files will be removed iff True.
        """
        super(IngestDirectoryDAG, self).__init__(
            *args,
            schedule_interval=schedule_interval,
            catchup=False,
            max_active_runs=1,
            default_args={
                # start_date is ideally utcnow()-schedule_interval but
                #   that gets tricky if schedule_interval is string.
                "start_date": datetime(2018, 7, 30),
                "retries": 0,
            },
            **kwargs
        )
        self.directory_path = directory_path
        assert len(load_kwargs_list) > 0
        self.load_kwargs_list = load_kwargs_list
        self.rm_loaded = rm_loaded
        self.add_ingest_tasks()

    def add_ingest_tasks(self):
        """
        adds ingest tasks for each item in self.load_kwargs_list

        NOTE: tasks are one after the other to reduce stress on file servers
           these could be parallel if disks & network could handle it.
        """
        prev_task = None
        for kwargs in self.load_kwargs_list:
            task_id = self.get_task_id(kwargs)
            kwargs = merge_dicts(
                self.COMMON_ARGS,
                kwargs
            )
            this_task = self.add_ingest_task(
                task_id=task_id,
                etl_load_args=kwargs
            )
            if prev_task is not None:
                # TODO: rm this if servers can handle it
                prev_task >> this_task
            prev_task = this_task

    def get_task_id(self, kwargs):
        """returns task id string for given kwargs list"""
        return "ingest_id{}_{}_n{}".format(
            kwargs.get("product_id", ""),
            kwargs.get("product_type_name", ""),
            len(self.tasks)
        )

    @staticmethod
    def _do_load_file(etl_load_args, **kwargs):
        """
        !!! DEPRECATED !!!
        runnable used to define an ingest task
        """
        # default args added to every ingest:
        load_args = dict(
            verbose=1,
            status_id=3,
            # TODO: should these be automaticaly added?
            # duplicates_ok=True,
            # nohash=True,
        )
        # explicitly passed args overwrite defaults above
        load_args.update(etl_load_args)
        print('running job:\nimars_etl.load(\n\t{}\n)'.format(load_args))
        imars_etl.load(
            **load_args
        )
        # TODO: check output
        #       Mark skipped if none loaded, else mark success.

    def add_ingest_task(self, task_id, etl_load_args):
        """
        """
        return PythonOperator(
            dag=self,
            task_id=task_id,
            op_args=[
                etl_load_args
            ],
            python_callable=self._do_load_directory,
        )

    def add_ingest_file(self, task_id, etl_load_args):
        """
        !!! DEPRECATED !!!
        manually add a file load directly
        """
        return PythonOperator(
            dag=self,
            task_id=task_id,
            op_args=[
                etl_load_args
            ],
            python_callable=self._do_load_file,
        )

    def _do_load_directory(self, load_kwargs):
        """
        a lot like running:

        python3 -m imars_etl find /srv/imars-objects/ftp-ingest \
            --product_id __PRODUCT_ID__ \
        | xargs -n 1 -i sh -c ' \
            python3 -m imars_etl -v load \
                --product_id __PRODUCT_ID__ \
                {} && \
            rm {}\
        '
        """
        print("loading output files into IMaRS data warehouse...")
        to_load = imars_etl.find(
            self.directory_path,
            **load_kwargs
        )
        results = []
        for filepath in to_load:
            # TODO: try/except?
            results.append(
                imars_etl.load(filepath=filepath, **load_kwargs)
            )
            if self.rm_loaded is True:
                print("trashing '{}'".format(filepath))
                shutil.move(
                    filepath,
                    '/srv/imars-objects/trash/' + filepath.replace('/', '.-')
                )
        # TODO: marks skipped unless something
        #           gets uploaded by using imars-etl python API directly.
        return results
