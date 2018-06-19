"""
checks for product which matches dhus_search_kwargs and is in the requested
roi.
"""
import requests

from imars_dags.dags.ingest.CoverageBranchOperator \
    import CoverageBranchOperator


def dhus_coverage_check(ds, **kwargs):
    """
    === examples:
    https://scihub.copernicus.eu/dhus/search?q=
      polarisationmode:VV%20AND%20
      footprint:%22Intersects(POLYGON((-4.53%2029.85,%2026.75%2029.85
      ,%2026.75%2046.80,-4.53%2046.80,-4.53%2029.85)))%22
    https://scihub.copernicus.eu/dhus/search?q=
      ingestiondate:%5bNOW-1DAY%20TO%20NOW%5d%20AND%20
      producttype:GRD
    === raw captured url from portal:
    https://scihub.copernicus.eu/s3/api/stub/products?filter=OLCI%20AND
      %20(%20footprint:%22Intersects(POLYGON((
      -20.48240612499999%2046.1852044749771,
      -13.802718624999999%2046.1852044749771,
      -13.802718624999999%2049.88547903687541,
      -20.48240612499999%2049.88547903687541,
      -20.48240612499999%2046.1852044749771)))%22%20)
      &offset=0&limit=25&sortedby=ingestiondate&order=desc
    """
    check_region = kwargs['roi']
    dhus_search_kwargs = kwargs['dhus_search_kwargs']



    exec_date = kwargs['execution_date']
    # TODO: filter with execution date?!?



    dhus_search_kwargs.setdefault('offset', 0)
    dhus_search_kwargs.setdefault('limit', 1)
    dhus_search_kwargs.setdefault('sortedby', 'ingestiondate')
    dhus_search_kwargs.setdefault('order', 'desc')

    if dhus_search_kwargs.get('filter') is None:
        check_region = kwargs['roi']
        west = check_region.lonmin,  # low l long
        south = check_region.latmin,  # low l lat
        east = check_region.lonmax,  # up r long
        north = check_region.latmax   # up r lat
        # raw:
        # 'filter': 'OLCI%20AND%20(%20footprint:%22Intersects(POLYGON((
        #   -20.48240612499999%2046.1852044749771,
        #   -13.802718624999999%2046.1852044749771,
        #   -13.802718624999999%2049.88547903687541,
        #   -20.48240612499999%2049.88547903687541,
        #   -20.48240612499999%2046.1852044749771)))%22%20)',
        # decoded:
        dhus_search_kwargs['filter'] = (
            'OLCI AND ( footprint:"Intersects(POLYGON(('
            '{} {},'.format(west, south) +  # w s | b l
            '{} {},'.format(east, south) +  # e s | b r
            '{} {},'.format(east, north) +  # e n | t r
            '{} {},'.format(west, north) +  # w n | t l
            '{} {}'.format(west, south) +  # w s | b l
            ')))" )'
        )

    else:  # dhus_search_kwargs['filter'] was set manually
        print(
            "WARN: manually setting filter, "
            "ROI will not be automatically included"
        )

    result = requests.get(
        'https://scihub.copernicus.eu/s3/api/stub/products',
        params=dhus_search_kwargs,
        auth=('s3guest', 's3guest')
    )

    if len(result) < 1:
        return kwargs['fail_branch_id']  # skip granule
    else:
        # TODO: this should write to imars_product_metadata instead?!?
        # === update (or create) the metadata ini file
        # path might have airflow macros, so we need to render
        task = kwargs['task']
        meta_path = kwargs['metadata_filepath']
        meta_path = task.render_template(
            '',
            meta_path,
            kwargs
        )
        with open(meta_path, 'w') as meta_file:
            meta_file.write(result.text)

        return kwargs['success_branch_id']  # download granule


class DHUSCoverageBranchOperator(CoverageBranchOperator):
    def __init__(
        self,
        roi=None,
        metadata_filepath=None,
        dhus_search_kwargs={},
        task_id='coverage_check',
        python_callable=dhus_coverage_check,
        op_kwargs={},
        **kwargs
    ):
        """
        dhus_search_kwargs : dict
            params dict to pass through to dhus request
            Example:
                {
                    'echo_collection_id': 'C1370679936-OB_DAAC',
                    'productType': 'OL_1_EFR___',
                },

        """
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
