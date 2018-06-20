"""
Checks the coverage of each granule using NASA's CMR
(Common Metadata Repository).
"""
# this package
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.settings import secrets  # NOTE: this file not in public repo!
from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.dag_classes.ingest.CoverageCheckDAG \
    import CoverageCheckDAG, add_load_cleanup_trigger, ROI_COVERED_BRANCH_ID
from imars_dags.dags.ingest.cmr.cmr_coverage_check \
    import cmr_coverage_check
from imars_dags.operators.DownloadFromMetadataFileOperator \
    import DownloadFromMetadataFileOperator
from imars_dags.operators.CoverageBranchOperator import CoverageBranchOperator


class CMRCoverageCheckDAG(CoverageCheckDAG):
    def __init__(
        self,
        region, region_short_name, region_id,
        product_id, product_short_name,
        cmr_search_kwargs,
        granule_len,
        **kwargs
    ):
        """
        Checks for coverage of given region using CMR iff the region is covered
        in the granule represented by the {{execution_date}}:
            1. loads the file using imars-etl
            2. triggers the given DAG (optional)

        Parameters:
        -----------
            cmr_search_kwargs : dict
                search_kwargs dict to pass to pyCMR.
                Example:
                    {'short_name': 'MYD01'}
        """
        super(CMRCoverageCheckDAG, self).__init__(
            dag_id=get_dag_id(
                __file__,
                region=region_short_name,
                dag_name="{}_cmr_coverage_check".format(product_short_name)
            ),
            schedule_interval=granule_len,
            catchup=True,
            max_active_runs=1,
            **kwargs
        )

        METADATA_FILE_FILEPATH = tmp_filepath(self.dag_id, "metadata.ini")
        DOWNLOADED_FILEPATH = tmp_filepath(self.dag_id, "cmr_download")
        coverage_check = CoverageBranchOperator(
            dag=self,
            op_kwargs={
                'cmr_search_kwargs': cmr_search_kwargs
            },
            roi=region,
            metadata_filepath=METADATA_FILE_FILEPATH,
            python_callable=cmr_coverage_check,
            task_id='coverage_check',
        )
        download_granule = DownloadFromMetadataFileOperator(
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
