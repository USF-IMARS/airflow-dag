# std libs
from datetime import datetime, timedelta

# deps
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import TimeDeltaSensor
from airflow import DAG

# this package
from imars_dags.util.globals import SLEEP_ARGS, QUEUE, DEFAULT_ARGS
from imars_dags.util.etl_tools.load import add_load
from imars_dags.util.etl_tools.cleanup import add_cleanup
from imars_dags.operators.MMTTriggerDagRunOperator \
    import MMTTriggerDagRunOperator


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
        default_args = DEFAULT_ARGS.copy()
        delay_ago = datetime.utcnow()-check_delay
        default_args.update({  # round to
            'start_date': delay_ago.replace(minute=0, second=0, microsecond=0),
        })
        super(CoverageCheckDAG, self).__init__(
            default_args=default_args,
            **kwargs
        )
        WaitForDataPublishSensor(
            dag=self, delta=check_delay
        )


class WaitForDataPublishSensor(TimeDeltaSensor):
    """
    =============================================================================
    === delay to wait for upstream data to become available.
    =============================================================================
    ie wait for download from OB.DAAC to complete.
    this makes sure that we don't try to run this DAG until `delta` amount of
    time past the `execution_date`
    (which is the datetime of the satellite recording).

    `delta` is the amount of time we expect between satellite measurement and
    the metadata being available in the CMR. Usually something like 2-48 hours.
    =============================================================================
    """

    def __init__(
        self,
        execution_timeout=SLEEP_ARGS['execution_timeout'],
        priority_weight=SLEEP_ARGS['priority_weight'],
        retries=SLEEP_ARGS['retries'],
        retry_delay=SLEEP_ARGS['retries'],
        task_id='wait_for_data_delay',
        **kwargs
    ):
        super(WaitForDataPublishSensor, self).__init__(
            priority_weight=priority_weight,
            retries=retries,
            retry_delay=retry_delay,
            task_id=task_id,
            **kwargs
        )


def add_load_cleanup_trigger(
    dag, DOWNLOADED_FILEPATH, METADATA_FILE_FILEPATH,
    region,
    product_id,
    area_id,
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
    region : imars_dags.regions.*
        region module (TODO: replace this)
    product_id : int
        product_id number from imars_product_metadata db
    area_id : int
        area_id number from imars_product_metadata db
    ingest_callback_dag_id : str
        id of DAG to trigger once ingest is complete.
        Example usages:
            1. trigger granuleProcDAG once granule ingested into data lake
            2. trigger downloadFileDAG once ingested into metadatadb
            3. trigger FileTriggerDAG immediately upon ingest
    """
    with dag as dag:
        download_granule = dag.get_task("download_granule")
        wait_for_data_delay = dag.get_task("wait_for_data_delay")
        coverage_check = dag.get_task("coverage_check")
        # ======================================================================
        to_load = [
            {
                "filepath": DOWNLOADED_FILEPATH,  # required!
                "verbose": 3,
                "product_id": product_id,
                # "time":"2016-02-12T16:25:18",
                # "datetime": datetime(2016,2,12,16,25,18),
                # NOTE: `is_day_pass` b/c of `day_night_flag` in CMR req.
                "json":
                    '{{'
                    'status_id":3,' +
                    '"is_day_pass":1,' +
                    '"area_id":{},' +
                    '"area_short_name":"{}"' +
                    '}}'.format(
                        area_id,
                        region.place_name
                    )
            }
        ]
        load_tasks = add_load(
            dag,
            to_load=to_load,
            upstream_operators=[download_granule]
        )
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
