"""
Generalized download operator based on PythonOperator.
"""

from datetime import timedelta
import json
import configparser

import requests
import imars_etl
from imars_etl.exceptions.NoMetadataMatchException \
    import NoMetadataMatchException
from airflow.operators.python_operator import PythonOperator


def get_uuid_from_dhus_json_file(json_metadata_filepath):
    with open(json_metadata_filepath) as m_file:
        return json.load(m_file)[0]['uuid']


def get_url_from_dhus_json_file(
    json_metadata_filepath,
    url_base='https://scihub.copernicus.eu/s3/odata/v1/Products'
):
    """Returns url read from DHUS json metadata file"""
    url_fmt_str = url_base + "('{uuid}')/$value"

    return url_fmt_str.format(
        uuid=get_uuid_from_dhus_json_file(json_metadata_filepath)
    )


def get_url_from_ini_file(ini_filepath):
    """Returns url read from custom ini file"""
    cfg = configparser.ConfigParser()
    cfg.read(ini_filepath)
    try:
        return cfg['myd01']['upstream_download_link']
    except KeyError:
        raise ValueError(
            "could not read upstream_download_link from ini file '{}'".format(
                ini_filepath
            )
        )


def file_not_yet_ingested(uuid):
    """returns true if file w/ given uuid is not in our system"""
    # check imars-etl db?
    try:
        result = imars_etl.get_metadata(
            sql="'uuid'='{}'".format(uuid),
            verbose=3,
        )
        assert len(result) > 0
        # assert len(result) == 1
        return False
    except NoMetadataMatchException:
        return True


def download_file(
    ds,
    username=None, password=None,
    url_getter=None,
    uuid_getter=get_uuid_from_dhus_json_file,
    templates_dict={},
    **kwargs
):
    metadata_file_filepath = templates_dict['metadata_file_filepath']
    downloaded_filepath = templates_dict['downloaded_filepath']
    print("downloading using metadata file '{}' to '{}'...".format(
        metadata_file_filepath,
        downloaded_filepath
    ))
    uuid = uuid_getter(metadata_file_filepath)
    # check for uuid already downloaded
    print("checking uuid '{}'".format(uuid))
    if file_not_yet_ingested(uuid):
        url = url_getter(metadata_file_filepath)
        print("downloading file from url '{}'".format(url))
        response = requests.get(
            url,
            auth=(username, password)
        )
        assert response.status_code == 200
        assert len(response.content) > 0
        with open(downloaded_filepath, "wb") as dl_file:
            dl_file.write(response.content)
    else:
        # TODO: set task state to skipped or raise error?
        print("skipping; file uuid already in system")


class DownloadFileOperator(PythonOperator):
    # template_fields = ('templates_dict', 'op_kwargs')

    def __init__(
        self,
        # required args:
        metadata_file_filepath, downloaded_filepath,
        # optional args:
        username='s3guest', password='s3guest',
        url_getter=get_url_from_dhus_json_file,

        # default args that passthrough to parent:
        task_id='download_file',
        trigger_rule='one_success',
        provide_context=True,
        retries=0,
        retry_delay=timedelta(minutes=1),
        **kwargs
    ):

        super(DownloadFileOperator, self).__init__(
            python_callable=download_file,
            op_kwargs={
                'username': username,
                'password': password,
                'url_getter': url_getter,
            },
            templates_dict={
                'metadata_file_filepath': metadata_file_filepath,
                'downloaded_filepath': downloaded_filepath,
            },

            trigger_rule=trigger_rule,
            task_id=task_id,
            provide_context=provide_context,
            retries=retries,
            retry_delay=retry_delay,
            **kwargs
        )
