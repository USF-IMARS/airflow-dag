"""
checks for product which matches dhus_search_kwargs and is in the requested
roi.
"""
import requests

from imars_dags.dags.ingest.CoverageBranchOperator \
    import CoverageBranchOperator


def dhus_coverage_check(ds, **kwargs):
    # https://scihub.copernicus.eu/dhus/search?q=
    #   polarisationmode:VV%20AND%20
    #   footprint:%22Intersects(POLYGON((-4.53%2029.85,%2026.75%2029.85,%2026.75%2046.80,-4.53%2046.80,-4.53%2029.85)))%22
    # https://scihub.copernicus.eu/dhus/search?q=
    #   ingestiondate:%5bNOW-1DAY%20TO%20NOW%5d%20AND%20
    #   producttype:GRD
    result = requests.get(
        'https://scihub.copernicus.eu/s3/api/stub/products',
        params={
            # 'filter': 'OLCI',  # %20AND%20(%20footprint:%22Intersects(
            #   POLYGON((-68.41794442091795%2018.587370193332475,
            #   -65.7408430169118%2018.587370193332475,
            #   -65.7408430169118%2021.005279979061285,
            #   -68.41794442091795%2021.005279979061285,
            #   -68.41794442091795%2018.587370193332475)))%22%20)
            'filter': 'OLCI AND ( footprint:"Intersects(POLYGON((' +
                '-66.0 20.0,' +  # noqa E131  W S bl
                '-65.0 20.0,' +  # E S br
                '-65.0 21.0,' +  # E N tr
                '-66.0 21.0,' +  # W N tl
                '-66.0 20.0' +  # W S bl
            ')))" )',
            # 'echo_collection_id': 'C1370679936-OB_DAAC',
            # 'productType': 'OL_1_EFR___',
            'offset': 0,
            'limit': 3,
            'sortedby': 'ingestiondate',
            'order': 'desc'
        },
        auth=('s3guest', 's3guest')
    )
    return result.json()


class DHUSCoverageBranchOperator(CoverageBranchOperator):
    def __init__(
        self,
        dhus_search_kwargs,
        roi,
        metadata_filepath,
        task_id='coverage_check',
        python_callable=dhus_coverage_check,
        op_kwargs={},
        **kwargs
    ):
        # === set dhus_search_kwargs within op_kwargs
        op_kwargs['dhus_search_kwargs'] = op_kwargs.get(
            'dhus_search_kwargs', dhus_search_kwargs
        )

        super(DHUSCoverageBranchOperator, self).__init__(
            metadata_filepath=metadata_filepath,
            python_callable=python_callable,
            roi=roi,
            task_id=task_id,
            op_kwargs=op_kwargs,
            **kwargs
        )
