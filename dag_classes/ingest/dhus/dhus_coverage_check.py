"""
A "latest-only" coverage check using ESA DHUS.

Checks for product which matches dhus_search_kwargs and is in the requested
roi.
This coverage check should always succeed, because it just returns the most
recent granule(s).
"""
from datetime import timedelta
import requests


def dhus_coverage_check(ds, granule_len=timedelta(minutes=3), **kwargs):
    """
    Parameters:
    -----------
    kwargs['dhus_search_kwargs'] : dict
        params dict to pass through to dhus request
        Example:
            {
                'echo_collection_id': 'C1370679936-OB_DAAC',
                'productType': 'OL_1_EFR___',
            },

    === example built queries:
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
    execution_date = kwargs['execution_date']

    dhus_search_kwargs.setdefault('offset', 0)
    dhus_search_kwargs.setdefault('limit', 1)
    dhus_search_kwargs.setdefault('sortedby', 'ingestiondate')
    dhus_search_kwargs.setdefault('order', 'desc')

    if dhus_search_kwargs.get('filter') is None:
        check_region = kwargs['roi']
        # raw:
        # 'filter': 'OLCI%20AND%20(%20footprint:%22Intersects(POLYGON((
        #   -20.48240612499999%2046.1852044749771,
        #   -13.802718624999999%2046.1852044749771,
        #   -13.802718624999999%2049.88547903687541,
        #   -20.48240612499999%2049.88547903687541,
        #   -20.48240612499999%2046.1852044749771)))%22%20)',
        # decoded:
        TIME_FMT = "%Y-%m-%dT%H:%M:%S.%f"  # iso 8601
        dhus_search_kwargs['filter'] = (
            'OLCI AND ( footprint:"Intersects(POLYGON(('
            '{west} {south},'  # w s | b l
            '{east} {south},'  # e s | b r
            '{east} {north},'  # e n | t r
            '{west} {north},'  # w n | t l
            '{west} {south}'  # w s | b l
            ')))" ) AND IngestionDate gt datetime"{earliest}"'
            ' AND IngestionDate lt datetime"{latest}"'.format(
                west=check_region.lonmin,  # low l long
                south=check_region.latmin,  # low l lat
                east=check_region.lonmax,  # up r long
                north=check_region.latmax,   # up r lat
                earliest=execution_date.strftime(TIME_FMT),
                latest=(execution_date + granule_len).strftime(TIME_FMT),
            )
        )
        print("filter:\n\t" + dhus_search_kwargs['filter'])

    else:  # dhus_search_kwargs['filter'] was set manually
        print(
            "WARN: manually setting filter, "
            "ROI will not be automatically included"
        )

    BASE_URL = 'https://scihub.copernicus.eu/s3/api/stub/products'
    result = requests.get(
        BASE_URL,
        params=dhus_search_kwargs,
        auth=('s3guest', 's3guest')
    )

    print('request sent to {}:\n\t{}'.format(
        BASE_URL,
        result.url
    ))

    print(result.status_code)
    if (result.status_code != 200):
        raise ValueError(
            "ERR: non-success response from server: {}".format(
                result.status_code
            )
        )

    print("response:\n\n--------------------\n{}\n--------------------".format(
        result.text
    ))

    if len(result.json()) < 1:
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
