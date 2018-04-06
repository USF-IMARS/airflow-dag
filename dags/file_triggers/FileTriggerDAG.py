"""
example usage:
```
product_type_id = 6
this_dag = FileTriggerDAG(
    product_type_id=6,
    dags_to_trigger=[
        "wv2_unzip"
    ]
)
```
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.sensors import SqlSensor
from airflow.operators.python_operator import PythonOperator

from imars_etl.get_metadata import get_metadata
from imars_etl.id_lookup import id_lookup

from imars_dags.operators.MMTTriggerDagRunOperator import MMTTriggerDagRunOperator


class STATUS:  # status IDs from imars_product_metadata.status
    # {status.short_name.upper()} = {status.id}
    TO_LOAD = 3
    STD     = 1

class FileTriggerDAG(DAG):

    def __init__(self, *args, **kwargs):
        """
        parameters:
        -----------
        product_type_id : int
            product_type_id for the product we are watching.
        dags_to_trigger : str[]
            list of DAG names to trigger when we get a new product.
        """
        self.product_type_id = kwargs.pop('product_type_id')
        self.dags_to_trigger = kwargs.pop('dags_to_trigger')

        # NOTE: catchup & max_active_runs prevent duplicate extractions
        kwargs['catchup']=False
        kwargs['max_active_runs']=1
        super(FileTriggerDAG, self).__init__(*args, **kwargs)
        self._add_file_trigger_tasks()

    def _add_file_trigger_tasks(self):
        with self as dag:
            # TODO: SQL watch for pid=={} & status==to_load
            # === mysql_sensor
            # =================================================================
            sql_selection="status={} AND product_type_id={};".format(
                STATUS.TO_LOAD,
                self.product_type_id
            )
            sql_str="SELECT id FROM file WHERE " + sql_selection
            check_for_to_loads = SqlSensor(
                task_id='check_for_to_loads',
                conn_id="imars_metadata",
                sql=sql_str,
                soft_fail=True,
            )
            # TODO: should set imars_product_metadata.status to "processing"
            #       to prevent duplicates?
            #       Not an issue so long as catchup=False & max_active_runs=1


            def get_file_metadata(**kwargs):
                # `ti.push()`es area_id & date_time from SQL
                ti = kwargs['ti']

                file_metadata = get_metadata(
                    {"sql": sql_selection}
                )

                # convert area_id to area_name
                file_metadata['area_name'] = imars_etl.id_lookup({
                    'table': 'area',
                    'value': ti.xcom_pull('area_id')
                })

                ti.xcom_push(key='file_metadata', value=file_metadata)
                # NOTE: can we just use the dict above?
                ti.xcom_push(key='area_id',   value=file_metadata['area_id'])
                ti.xcom_push(key='area_name',   value=file_metadata['area_name'])
                ti.xcom_push(key='date_time', value=file_metadata['date_time'])
                return fname

            get_file_metadata = PythonOperator(
                task_id='get_file_metadata',
                provide_context=True,
                python_callable=get_file_metadata,
            )



            # TODO: trigger dag(s) for this product & for this region
            # area_id = "GOM"  # TODO: ti.pull()
            # exec_date = "2018-01-01T01:01"  # TODO: ti.pull()
            # for processing_dag_name in self.dags_to_trigger:
            #     # processing_dag_name is root dag, but each region has a dag
            #     dag_to_trigger="{}_{}".format(area_id, processing_dag_name)
            #     trigger_dag_operator_id = "trigger_{}".format(dag_to_trigger)
            #
            #     trigger_processing_REGION = MMTTriggerDagRunOperator(
            #         trigger_dag_id=trigger_dag_operator_id,
            #         python_callable=lambda context, dag_run_obj: dag_run_obj,
            #         execution_date="{{params.exec_date}}",
            #         task_id=dag_to_trigger,
            #         params={
            #             'exec_date': exec_date
            #         },
            #         retries=1,
            #         retry_delay=timedelta(minutes=2)
            #     )
                # TODO:
                # sql_watch >> trigger_processing_REGION

            # TODO: update metadata db
