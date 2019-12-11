"""
Methods for parsing json responses from ESA DHUS.
"""

import json


def get_uuid(json_metadata_filepath):
    with open(json_metadata_filepath) as m_file:
        return json.load(m_file)['products'][0]['uuid']


def get_url(
    json_metadata_filepath,
    url_base='https://scihub.copernicus.eu/s3/odata/v1/Products'
):
    """Returns url read from DHUS json metadata file"""
    url_fmt_str = url_base + "('{uuid}')/$value"

    return url_fmt_str.format(
        uuid=get_uuid(json_metadata_filepath)
    )
