# std libs
from datetime import datetime, timedelta

# deps
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

# this package
from imars_dags.util.globals import QUEUE
from imars_dags.util.get_default_args import get_default_args
from imars_dags.util.etl_tools.load import add_load
from imars_dags.util.etl_tools.cleanup import add_cleanup
from imars_dags.operators.MMTTriggerDagRunOperator \
    import MMTTriggerDagRunOperator
from imars_dags.operators.WaitForDataPublishSensor \
    import WaitForDataPublishSensor


ROI_COVERED_BRANCH_ID = 'download_granule'
ROI_NOT_COVERED_BRANCH_ID = 'skip_granule'


class CoverageCheckDAG(DAG):
    """
    Parameters:
    -----------
        check_delay : datetime.timedelta
            amount of time to wait before checking
    """
    def __init__(
        self,
        check_delay=timedelta(seconds=0),
        **kwargs
    ):
        delay_ago = datetime.utcnow()-check_delay
        super(CoverageCheckDAG, self).__init__(
            default_args=get_default_args(
                start_date=delay_ago.replace(  # round to
                    minute=0, second=0, microsecond=0
                ),
            ),
            **kwargs
        )
        WaitForDataPublishSensor(
            dag=self, delta=check_delay
        )


def validate_op_obj(dag, op_obj):
    """
    if op_obj is str, then gets op_obj using op_obj as task_id.
    """
    if type(op_obj) == str:
        return dag.get_task(op_obj)
    else:
        return op_obj


def add_load_cleanup_trigger(
    dag, DOWNLOADED_FILEPATH, METADATA_FILE_FILEPATH,
    region,
    product_id,
    area_id,
    download_granule_op,
    coverage_check_op,
    load_ops=None,
    uuid_getter=lambda *args, **kwargs: 'NULL',
    wait_for_data_delay_op=WaitForDataPublishSensor.DEFAULT_TASK_ID,
    ingest_callback_dag_id=None
):
    """
    adds tasks to:
        1. load DOWNLOADED_FILEPATH into imars-etl data warehouse
        2. clean up tmp files left behind
        3. (optional) trigger a "callback" DAG

    Parameters:
    ----------
    dag : airflow.DAG
        dag to add tasks to
    DOWNLOADED_FILEPATH : TODO
    METADATA_FILE_FILEPATH : TODO
    region : imars_dags.regions.*
        region module (TODO: replace this)
    product_id : int
        product_id number from imars_product_metadata db
    area_id : int
        area_id number from imars_product_metadata db
    download_granule_op : airflow.Operator || str
    coverage_check_op : airflow.Operator || str
    wait_for_data_delay_op : airflow.Operator || str
    ingest_callback_dag_id : str
        id of DAG to trigger once ingest is complete.
        Example usages:
            1. trigger granuleProcDAG once granule ingested into data lake
            2. trigger downloadFileDAG once ingested into metadatadb
            3. trigger FileTriggerDAG immediately upon ingest
    """
    download_granule = validate_op_obj(dag, download_granule_op)
    wait_for_data_delay = validate_op_obj(dag, wait_for_data_delay_op)
    coverage_check = validate_op_obj(dag, coverage_check_op)
    with dag as dag:
        # ======================================================================
        if load_ops is None:
            to_load = [
                {
                    "filepath": DOWNLOADED_FILEPATH,  # required!
                    "verbose": 3,
                    "product_id": product_id,
                    # NOTE: `is_day_pass` b/c of `day_night_flag` in CMR req.
                    # "metadata_file": METADATA_FILE_FILEPATH,
                    "json": (
                        '{{' +
                        '"status_id":3,' +
                        '"is_day_pass":1,' +
                        '"area_id":{},' +
                        '"area_short_name":"{}"' +
                        '}}'
                    ).format(
                        area_id,
                        region.place_name,
                    )
                }
            ]
            load_tasks = add_load(
                dag,
                to_load=to_load,
                upstream_operators=[download_granule]
            )
        else:
            load_tasks = load_ops
        assert len(load_tasks) == 1  # right?
        load_downloaded_file = load_tasks[0]  # no point looping
        # ======================================================================
        skip_granule = DummyOperator(
            task_id=ROI_NOT_COVERED_BRANCH_ID,
            trigger_rule='one_success'
        )
        # ======================================================================
        if ingest_callback_dag_id is not None:
            trigger_callback_dag_id = 'trigger_' + ingest_callback_dag_id
            trigger_callback_dag = MMTTriggerDagRunOperator(
                trigger_dag_id=ingest_callback_dag_id,
                python_callable=lambda context, dag_run_obj: dag_run_obj,
                exec_date_fmt_str='%Y-%m-%d %H:%M:%S',
                # TODO: include real microseconds in execution_date?
                execution_date="{{execution_date}}",
                task_id=trigger_callback_dag_id,
                params={
                    'roi': region
                },
                retries=0,
                retry_delay=timedelta(hours=3),
                queue=QUEUE.PYCMR
            )
            load_downloaded_file >> trigger_callback_dag
        # ======================================================================
        wait_for_data_delay >> coverage_check
        coverage_check >> skip_granule
        coverage_check >> download_granule
        # implied by upstream_operators=[download_granule] :
        # download_granule >> load_downloaded_file

        add_cleanup(
            dag,
            to_cleanup=[DOWNLOADED_FILEPATH, METADATA_FILE_FILEPATH],
            upstream_operators=[download_granule, coverage_check]
        )
