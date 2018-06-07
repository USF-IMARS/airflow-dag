"""
Airflow processing pipeline definition for MODIS aqua per-pass processing.
Checks the coverage of each granule and triggers pass-level processing for each
region.
"""
# std libs
from datetime import timedelta, datetime
import subprocess
import configparser
import os

# deps
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# this package
from imars_dags.operators.MMTTriggerDagRunOperator import MMTTriggerDagRunOperator
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, SLEEP_ARGS
from imars_dags.settings import secrets  # NOTE: this file not in public repo!
from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.util.etl_tools.load import add_load
from imars_dags.util.etl_tools.cleanup import add_cleanup
from imars_dags.dags.ingest.CoverageCheckDAG import WaitForDataPublishSensor
from imars_dags.dags.ingest.cmr.CMRCoverageBranchOperator import CMRCoverageBranchOperator

schedule_interval=timedelta(minutes=5)

def add_tasks(dag, region, product_id, area_id, cmr_search_kwargs, check_delay,
        ingest_callback_dag_id=None
    ):
    """
    Checks for coverage of given region using CMR iff the region is covered in
    the granule represented by the {{execution_date}}:
        1. loads the file using imars-etl
        2. triggers the given DAG (optional)

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
    cmr_search_kwargs : dict
        search_kwargs dict to pass to pyCMR.
        Example:
            {'short_name': 'MYD01'}
    ingest_callback_dag_id : str
        id of DAG to trigger once ingest is complete.
        Example usages:
            1. trigger granuleProcDAG once granule is ingested into data lake
            2. trigger downloadFileDAG once granule is ingested into metadatadb
            3. trigger FileTriggerDAG immediately upon ingest
    """
    with dag as dag:
        wait_for_data_delay = WaitForDataPublishSensor(delta=check_delay)
        METADATA_FILE_FILEPATH = tmp_filepath(dag.dag_id, "metadata.ini")
        coverage_check = CMRCoverageBranchOperator(
                cmr_search_kwargs=cmr_search_kwargs,
                roi=region,
                metadata_filepath=METADATA_FILE_FILEPATH,
        )
        # ======================================================================
        # reads the download url from a metadata file created in the last step and
        # downloads the file iff the file does not already exist.
        DOWNLOADED_FILEPATH = tmp_filepath(dag.dag_id, "cmr_download")
        download_granule = BashOperator(
            task_id=CMRCoverageBranchOperator.ROI_COVERED_BRANCH_ID,
            bash_command="""
                METADATA_FILE="""+METADATA_FILE_FILEPATH+""" &&
                OUT_PATH="""+DOWNLOADED_FILEPATH+""" &&
                FILE_URL=$(grep "^upstream_download_link" $METADATA_FILE | cut -d'=' -f2-) &&
                curl --user {{params.username}}:{{params.password}} -f $FILE_URL -o $OUT_PATH &&
                [[ -s $OUT_PATH ]]
            """,
            params={
                "username": secrets.ESDIS_USER,
                "password": secrets.ESDIS_PASS,
            },
            trigger_rule='one_success'
        )
        # ======================================================================
        to_load = [
            {
                "filepath":DOWNLOADED_FILEPATH,  # required!
                "verbose":3,
                "product_id":product_id,
                # "time":"2016-02-12T16:25:18",
                # "datetime": datetime(2016,2,12,16,25,18),
                # NOTE: `is_day_pass` below b/c of `day_night_flag` in CMR req.
                "json":'{{"status_id":3,"is_day_pass":1,"area_id":{},"area_short_name":"{}"}}'.format(
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
        load_downloaded_file = load_tasks[0]  # no point looping just use this
        # ======================================================================
        skip_granule = DummyOperator(
            task_id=CMRCoverageBranchOperator.ROI_NOT_COVERED_BRANCH_ID,
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
                    'roi':region
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
        # download_granule >> load_downloaded_file  # implied by upstream_operators=[download_granule]

        cleanup_task = add_cleanup(
            dag,
            to_cleanup=[DOWNLOADED_FILEPATH, METADATA_FILE_FILEPATH],
            upstream_operators=[download_granule, coverage_check]
        )
