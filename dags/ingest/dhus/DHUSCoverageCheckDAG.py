"""
Checks the coverage of each granule using ESA's DHUS (Data HUb Software)
aka copernicus open access hub.
* https://scihub.copernicus.eu/dhus/#/home
* https://sentineldatahub.github.io/DataHubSystem/
"""

from imars_dags.settings import secrets  # NOTE: this file not in public repo!
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.dags.ingest.dhus.DHUSCoverageBranchOperator \
    import DHUSCoverageBranchOperator
from imars_dags.operators.DownloadFromJSONMetadataOperator \
    import DownloadFromJSONMetadataOperator
from imars_dags.dags.ingest.CoverageCheckDAG \
    import CoverageCheckDAG, add_load_cleanup_trigger, ROI_COVERED_BRANCH_ID


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

        coverage_check = DHUSCoverageBranchOperator(
            dag=self,
            dhus_search_kwargs=dhus_search_kwargs,
            roi=region,
            metadata_filepath=METADATA_FILE_FILEPATH,
        )
        download_granule = DownloadFromJSONMetadataOperator(
            METADATA_FILE_FILEPATH,
            DOWNLOADED_FILEPATH,
            dag=self,
            username=secrets.ESDIS_USER,
            password=secrets.ESDIS_PASS,
            task_id=ROI_COVERED_BRANCH_ID
        )

        add_load_cleanup_trigger(
            self,
            DOWNLOADED_FILEPATH,
            METADATA_FILE_FILEPATH,
            region=region,
            product_id=product_id,
            area_id=region_id,
            download_granule_op=download_granule,
            coverage_check_op=coverage_check
        )