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
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.mysql_operator import MySqlOperator

from imars_etl.select import select
from imars_etl.id_lookup import id_lookup

from imars_dags.operators.MMTTriggerDagRunOperator \
    import MMTTriggerDagRunOperator


METADATA_CONN_ID = "imars_metadata"


def get_subdag(
    parent_id, child_dag_name, start_date,
    product_ids,
    dags_to_trigger,
    area_names,
    poke_interval,
):
    # Dag is returned by a factory method
    return FileTriggerSubDAG(
        '%s.%s' % (parent_id, child_dag_name),
        start_date=start_date,
        product_ids=product_ids,
        dags_to_trigger=dags_to_trigger,
        area_names=area_names,
        poke_interval=poke_interval,
    )


def get_sql_selection(product_ids):
    VALID_STATUS_IDS = [1, 3, 4]
    return "status_id IN ({}) AND product_id IN ({})".format(
        ",".join(map(str, VALID_STATUS_IDS)),
        ",".join(map(str, product_ids))
    )


class FileTriggerSubDAG(DAG):
    def __init__(
        self,
        *args,
        area_names=[], product_ids, dags_to_trigger,
        # NOTE: catchup & max_active_runs prevent duplicate extractions
        catchup=False,
        max_active_runs=1,
        poke_interval,
        **kwargs
    ):
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
        self.product_ids = product_ids
        self.dags_to_trigger = dags_to_trigger
        self.poke_interval = poke_interval

        super(FileTriggerSubDAG, self).__init__(
            *args,
            catchup=catchup,
            max_active_runs=max_active_runs,
            **kwargs
        )
        self._add_file_trigger_tasks()

    def _add_file_trigger_tasks(self):
        with self as dag:  # noqa F841
            sql_selection = get_sql_selection(self.product_ids)
            # TODO: re-work this using MySqlOperator (NOT imars_etl.select) ?

            """
            === get_file_metadata
            =================================================================
            retrieves metadata from db & stores it in ti xcom for other
            operators to use.
            """
            def get_file_metadata(ti=None, **kwargs):
                # `ti.push()`es area_id & date_time from SQL
                print("sql:\n\t{}".format(sql_selection))
                file_metadata = select(
                        sql_selection,
                        first=True,
                )
                # print("\n\tmeta:\n\t{}\n".format(file_metadata))
                # logging.info("\n\n\tmeta:\n\t{}".format(file_metadata))
                # convert area_id to area_name
                file_metadata['area_name'] = id_lookup(
                    table='area',
                    value=file_metadata['area_id']
                )

                ti.xcom_push(key='file_metadata', value=file_metadata)
                # NOTE: can we just use the dict above?
                ti.xcom_push(
                    key='area_id',   value=file_metadata['area_id']
                )
                ti.xcom_push(
                    key='area_name', value=file_metadata['area_name']
                )
                ti.xcom_push(
                    key='date_time', value=file_metadata['date_time']
                )
                print("\n\tenhanced meta:\n\t{}\n".format(file_metadata))
                # logging.info(file_metadata)
                return file_metadata

            get_file_metadata = PythonOperator(
                task_id='get_file_metadata',
                provide_context=True,
                python_callable=get_file_metadata,
            )

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
                sql=(
                    ' UPDATE file SET status_id=1 WHERE '
                    'filepath="{{ '
                        'ti.xcom_pull('
                            'task_ids="get_file_metadata"'  # noqa E131
                        ')["filepath"] '
                    '}}" '
                ),
                mysql_conn_id=METADATA_CONN_ID,
                autocommit=False,  # TODO: True?
                parameters=None,
                trigger_rule="one_success"
            )
            # === else failed
            # TODO: use STATUS.ERROR here
            # TODO: re-enable this?
            set_product_status_to_err = MySqlOperator(
                task_id="set_product_status_to_err",
                sql=(
                    ' UPDATE file SET status_id=4 WHERE '
                    ' filepath="{{ '
                    ' ti.xcom_pull(task_ids="get_file_metadata")["filepath"] '
                    '}}" '
                ),
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
                #   BranchPythonOperator above can trigger several
                #   operators grouped by ROI under ${ROI}_dummy.
                ROI_dummy = DummyOperator(
                    task_id=roi_name+"_dummy",
                    trigger_rule='one_success'
                )
                branch_to_correct_region >> ROI_dummy
                if len(self.dags_to_trigger) > 0:
                    for processing_dag_name in self.dags_to_trigger:
                        # processing_dag_name is root dag,
                        # but each region has a dag
                        dag_to_trigger = "{}_{}".format(
                            processing_dag_name, roi_name
                        )
                        trigger_dag_operator_id = "trigger_{}".format(
                            dag_to_trigger
                        )
                        ROI_processing_DAG = MMTTriggerDagRunOperator(
                            task_id=trigger_dag_operator_id,
                            python_callable=(
                                lambda context, dag_run_obj: dag_run_obj
                            ),
                            retries=0,
                            retry_delay=timedelta(minutes=2),
                            trigger_dag_id=dag_to_trigger,
                            execution_date=("{{"  # noqa E128
                                " ti.xcom_pull("
                                    "task_ids='get_file_metadata', "
                                    "key='date_time'"
                                ").strftime('%Y-%m-%d %H:%M:%S.%f') "
                            "}}"),
                        )
                        ROI_dummy >> ROI_processing_DAG
                        ROI_processing_DAG >> set_product_status_to_std
                        ROI_processing_DAG >> set_product_status_to_err
                else:  # no dags_to_trigger means set it std and do nothing
                    ROI_dummy >> set_product_status_to_std
                    ROI_dummy >> set_product_status_to_err