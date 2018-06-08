"""
Airflow processing pipeline definition for MODIS aqua per-pass processing.
Checks the coverage of each granule and triggers pass-level processing for each
region.
"""
# std libs
from datetime import datetime, timedelta

# deps
from airflow.operators.dummy_operator import DummyOperator

# this package
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.operators.MMTTriggerDagRunOperator \
    import MMTTriggerDagRunOperator
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS
from imars_dags.settings import secrets  # NOTE: this file not in public repo!
from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.util.etl_tools.load import add_load
from imars_dags.util.etl_tools.cleanup import add_cleanup
from imars_dags.dags.ingest.CoverageCheckDAG import CoverageCheckDAG
from imars_dags.dags.ingest.cmr.CMRCoverageBranchOperator \
    import CMRCoverageBranchOperator
from imars_dags.dags.ingest.DownloadFromMetadataFileOperator \
    import DownloadFromMetadataFileOperator


schedule_interval = timedelta(minutes=5)


class CMRCoverageCheckDAG(CoverageCheckDAG):
    def __init__(
        self,
        region, region_short_name, region_id,
        product_id, product_short_name,
        cmr_search_kwargs,
        granule_len,
        check_delay,
        **kwargs
    ):
        default_args = DEFAULT_ARGS.copy()
        delay_ago = datetime.utcnow()-check_delay
        default_args.update({  # round to
            'start_date': delay_ago.replace(minute=0, second=0, microsecond=0),
        })

        super(CMRCoverageCheckDAG, self).__init__(
            check_delay,
            dag_id=get_dag_id(
                __file__,
                region=region_short_name,
                dag_name="{}_cmr_coverage_check".format(product_short_name)
            ),
            default_args=default_args,
            schedule_interval=granule_len,
            catchup=True,
            max_active_runs=1,
            **kwargs
        )

        self.add_tasks(
            region=region,
            product_id=product_id,
            area_id=region_id,
            cmr_search_kwargs=cmr_search_kwargs,
            check_delay=check_delay,
        )

    def add_tasks(
        self, region, product_id, area_id, cmr_search_kwargs, check_delay,
        ingest_callback_dag_id=None
    ):
        """
        Checks for coverage of given region using CMR iff the region is covered
        in the granule represented by the {{execution_date}}:
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
                1. trigger granuleProcDAG once granule ingested into data lake
                2. trigger downloadFileDAG once ingested into metadatadb
                3. trigger FileTriggerDAG immediately upon ingest
        """
        with self as dag:
            METADATA_FILE_FILEPATH = tmp_filepath(dag.dag_id, "metadata.ini")
            coverage_check = CMRCoverageBranchOperator(
                    cmr_search_kwargs=cmr_search_kwargs,
                    roi=region,
                    metadata_filepath=METADATA_FILE_FILEPATH,
            )
            # ======================================================================
            DOWNLOADED_FILEPATH = tmp_filepath(dag.dag_id, "cmr_download")
            download_granule = DownloadFromMetadataFileOperator(
                METADATA_FILE_FILEPATH,
                DOWNLOADED_FILEPATH,
                username=secrets.ESDIS_USER,
                password=secrets.ESDIS_PASS,
                task_id=CMRCoverageBranchOperator.ROI_COVERED_BRANCH_ID
            )
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
                        'roi': region
                    },
                    retries=0,
                    retry_delay=timedelta(hours=3),
                    queue=QUEUE.PYCMR
                )
                load_downloaded_file >> trigger_callback_dag
            # ======================================================================
            self.get_task("wait_for_data_delay") >> coverage_check
            coverage_check >> skip_granule
            coverage_check >> download_granule
            # implied by upstream_operators=[download_granule] :
            # download_granule >> load_downloaded_file

            add_cleanup(
                self,
                to_cleanup=[DOWNLOADED_FILEPATH, METADATA_FILE_FILEPATH],
                upstream_operators=[download_granule, coverage_check]
            )
