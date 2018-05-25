"""
example usage:
```
# trigger wv2_unzip for files with product_id = 6
this_dag = FileTriggerDAG(
    product_ids=[6],
    dags_to_trigger=[
        "wv2_unzip"
    ]
)
```
"""
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.sensors import SqlSensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator

from imars_etl.get_metadata import get_metadata
from imars_etl.id_lookup import id_lookup
from imars_dags.util.globals import DEFAULT_ARGS
from imars_dags.util.list_to_sql_or import list_to_sql_or

from imars_dags.operators.MMTTriggerDagRunOperator import MMTTriggerDagRunOperator


class STATUS:  # status IDs from imars_product_metadata.status
    # {status.short_name.upper()} = {status.id}
    STD     = 1
    EXTERNAL= 2
    TO_LOAD = 3
    ERROR   = 4

METADATA_CONN_ID="imars_test_metadata"

class FileTriggerDAG(DAG):
    DAWN_OF_TIME = datetime(2018, 5, 5, 5, 5)  # any date in past is fine
    SCHEDULE_INTERVAL = timedelta(minutes=1)
    # SCHEDULE_INTERVAL must be >= POKE_INTERVAL.
    # Also: NOTE: SCHEDULE_INTERVAL sets the maximum frequency that products
    #   can be ingested at 1 per SCHEDULE_INTERVAL.
    POKE_INTERVAL = 60  # use higher value for less load on prod meta server
    def __init__(self, area_names=['na'], *args, **kwargs):
        """
        parameters:
        -----------
        product_ids : int[]
            list of `product_ids` for the product we are watching.
        dags_to_trigger : str[]
            list of DAG names to trigger when we get a new product.
        area_names: str[]
            list of RoIs that we should consider triggering
            example: ['na', 'gom', 'fgbnms']

        """
        self.area_names = area_names
        self.product_ids = kwargs.pop('product_ids')
        self.dags_to_trigger = kwargs.pop('dags_to_trigger')
        # === overload some arguments TODO: warn or something???
        # NOTE: catchup & max_active_runs prevent duplicate extractions
        kwargs['catchup']=False
        kwargs['max_active_runs']=1

        # === set arguments if ommitted
        # TODO: I think these can be done in the function declaration with
        #       standard default argument notation (default_args=def_def_args)
        def_def_args = DEFAULT_ARGS.copy()
        def_def_args.update({
            'start_date': self.DAWN_OF_TIME,
            'retries': 0,
        })
        kwargs['default_args'] = getattr(
            kwargs,
            'default_args',
            def_def_args
        )

        kwargs['schedule_interval'] = getattr(
            kwargs,
            'schedule_interval',
            self.SCHEDULE_INTERVAL
        )
        self.schedule_interval = kwargs['schedule_interval']

        super(FileTriggerDAG, self).__init__(*args, **kwargs)
        self._add_file_trigger_tasks()

    def _add_file_trigger_tasks(self):
        with self as dag:
            # TODO: SQL watch for pid=={} & status_id==to_load
            # === mysql_sensor
            # =================================================================
            sql_selection="status_id={} AND {};".format(
                STATUS.TO_LOAD,
                list_to_sql_or('product_id', self.product_ids)
            )
            sql_str="SELECT id FROM file WHERE " + sql_selection
            check_for_to_loads = SqlSensor(
                task_id='check_for_to_loads',
                conn_id=METADATA_CONN_ID,
                sql=sql_str,
                soft_fail=True,
                poke_interval=self.POKE_INTERVAL,
                # timeout matches schedule_interval b/c we always want 1 running
                timeout=self.schedule_interval.total_seconds()
            )
            # TODO: should set imars_product_metadata.status to "processing"
            #       to prevent duplicates?
            #       Not an issue so long as catchup=False & max_active_runs=1

            """
            === get_file_metadata
            =================================================================
            retrieves metadata from db & stores it in ti xcom for other
            operators to use.
            """
            def get_file_metadata(**kwargs):
                # `ti.push()`es area_id & date_time from SQL
                ti = kwargs['ti']
                # print("sql:\n\t{}".format(sql_selection))
                file_metadata = get_metadata(
                    {
                        "sql": sql_selection,
                        "first": True
                    }
                )
                # print("\n\tmeta:\n\t{}\n".format(file_metadata))
                # logging.info("\n\n\tmeta:\n\t{}".format(file_metadata))
                # convert area_id to area_name
                file_metadata['area_name'] = id_lookup({
                    'table': 'area',
                    'value': file_metadata['area_id']
                })

                ti.xcom_push(key='file_metadata', value=file_metadata)
                # NOTE: can we just use the dict above?
                ti.xcom_push(key='area_id',   value=file_metadata['area_id'])
                ti.xcom_push(key='area_name', value=file_metadata['area_name'])
                ti.xcom_push(key='date_time', value=file_metadata['date_time'])
                print("\n\tenhanced meta:\n\t{}\n".format(file_metadata))
                # logging.info(file_metadata)
                return file_metadata

            get_file_metadata = PythonOperator(
                task_id='get_file_metadata',
                provide_context=True,
                python_callable=get_file_metadata,
            )
            check_for_to_loads >> get_file_metadata

            """
            === branch_to_correct_region
            =================================================================
            uses region_name from metadata to trigger the correct dag(s)
            """
            def branch_to_correct_region(**kwargs):
                ti = kwargs['ti']
                area_name = ti.xcom_pull(
                    task_ids='get_file_metadata',
                    key='area_name'
                )
                return area_name + "_dummy"

            branch_to_correct_region = BranchPythonOperator(
                task_id="branch_to_correct_region",
                provide_context=True,
                python_callable=branch_to_correct_region,
            )
            get_file_metadata >> branch_to_correct_region

            """
            === update metadata db
            =================================================================
            """
            # === if success
            # TODO: use STATUS.STD here
            set_product_status_to_std = MySqlOperator(
                task_id="set_product_status_to_std",
                sql=""" UPDATE file SET status_id=1 WHERE filepath="{{ ti.xcom_pull(task_ids="get_file_metadata")["filepath"] }}" """,
                mysql_conn_id=METADATA_CONN_ID,
                autocommit=False,  # TODO: True?
                parameters=None,
                trigger_rule="one_success"
            )
            # === else failed
            # TODO: use STATUS.ERROR here
            set_product_status_to_err = MySqlOperator(
                task_id="set_product_status_to_err",
                sql=""" UPDATE file SET status_id=4 WHERE filepath="{{ ti.xcom_pull(task_ids="get_file_metadata")["filepath"] }}" """,
                mysql_conn_id=METADATA_CONN_ID,
                autocommit=False,  # TODO: True?
                parameters=None,
                trigger_rule="one_failed"
            )

            """
            === trigger region processing dags
            =================================================================
            """
            # trigger dag(s) for this product & for this region
            for roi_name in self.area_names:
                # the dummy operator is just a choke point so the
                #   BranchPythonOperator above can trigger several operators
                #   grouped by ROI under ${ROI}_dummy.
                ROI_dummy = DummyOperator(
                    task_id=roi_name+"_dummy",
                    trigger_rule='one_success'
                )
                branch_to_correct_region >> ROI_dummy
                if len(self.dags_to_trigger) > 0:
                    for processing_dag_name in self.dags_to_trigger:
                        # processing_dag_name is root dag, but each region has a dag
                        dag_to_trigger="{}_{}".format(processing_dag_name, roi_name)
                        trigger_dag_operator_id = "trigger_{}".format(dag_to_trigger)
                        ROI_processing_DAG = MMTTriggerDagRunOperator(
                            task_id=trigger_dag_operator_id,
                            python_callable=lambda context, dag_run_obj: dag_run_obj,
                            retries=1,
                            retry_delay=timedelta(minutes=2),
                            trigger_dag_id=dag_to_trigger,
                            execution_date="{{ ti.xcom_pull(task_ids='get_file_metadata', key='date_time').strftime('%Y-%m-%d %H:%M:%S') }}",
                        )
                        ROI_dummy >> ROI_processing_DAG
                        ROI_processing_DAG >> set_product_status_to_std
                        ROI_processing_DAG >> set_product_status_to_err
                else:  # no dags_to_trigger means just set it std and do nothing
                    ROI_dummy >> set_product_status_to_std
                    ROI_dummy >> set_product_status_to_err
