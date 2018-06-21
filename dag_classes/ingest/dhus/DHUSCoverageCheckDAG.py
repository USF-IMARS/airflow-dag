"""
A "latest-only" coverage check using ESA DHUS.

Checks the coverage of each granule using ESA's DHUS (Data HUb Software)
aka copernicus open access hub.
* https://scihub.copernicus.eu/dhus/#/home
* https://sentineldatahub.github.io/DataHubSystem/
"""

from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.operators.CoverageBranchOperator \
    import CoverageBranchOperator
from imars_dags.dag_classes.ingest.CoverageCheckDAG import CoverageCheckDAG
from imars_dags.dag_classes.ingest.CoverageCheckDAG \
    import add_load_cleanup_trigger
from imars_dags.dag_classes.ingest.CoverageCheckDAG \
    import ROI_COVERED_BRANCH_ID
from imars_dags.dag_classes.ingest.dhus.dhus_coverage_check \
    import dhus_coverage_check
from imars_dags.operators.DownloadFileOperator.DownloadFileOperator \
    import DownloadFileOperator
from imars_dags.operators.DownloadFileOperator import dhus_json_driver


class DHUSCoverageCheckDAG(CoverageCheckDAG):
    def __init__(
        self,
        region, region_short_name, region_id,
        product_id, product_short_name,
        dhus_search_kwargs,
        granule_len,
        **kwargs
    ):
        super(DHUSCoverageCheckDAG, self).__init__(
            dag_id=get_dag_id(
                __file__,
                region=region_short_name,
                dag_name="{}_dhus_coverage_check".format(product_short_name)
            ),
            schedule_interval=granule_len,
            catchup=True,
            max_active_runs=1,
            **kwargs
        )
        METADATA_FILE_FILEPATH = tmp_filepath(self.dag_id, "searchresult.json")
        DOWNLOADED_FILEPATH = tmp_filepath(self.dag_id, "dhus_download")

        coverage_check = CoverageBranchOperator(
            dag=self,
            op_kwargs={
                'dhus_search_kwargs': dhus_search_kwargs,
            },
            roi=region,
            metadata_filepath=METADATA_FILE_FILEPATH,
            python_callable=dhus_coverage_check,
            task_id='coverage_check',
        )
        download_granule = DownloadFileOperator(
            METADATA_FILE_FILEPATH,
            DOWNLOADED_FILEPATH,
            uuid_getter=dhus_json_driver.get_uuid,
            url_getter=dhus_json_driver.get_url,
            dag=self,
            username='s3guest',
            password='s3guest',
            task_id=ROI_COVERED_BRANCH_ID,
        )

        add_load_cleanup_trigger(
            self,
            DOWNLOADED_FILEPATH,
            METADATA_FILE_FILEPATH,
            region=region,
            product_id=product_id,
            area_id=region_id,
            download_granule_op=download_granule,
            coverage_check_op=coverage_check,
            uuid_getter=dhus_json_driver.get_uuid,
        )
