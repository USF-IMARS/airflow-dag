"""
very simple download using curl from a DHUS metadata json response file.
"""
from airflow.operators.bash_operator import BashOperator


class DownloadFromJSONMetadataOperator(BashOperator):
    def __init__(
        self,
        # required args:
        metadata_file_filepath, downloaded_filepath,
        # optional args:
        username=None, password=None,

        # default args that passthrough to parent:
        trigger_rule='one_success',
        **kwargs
    ):
        if username is None and password is None:
            auth_string = ""
        else:
            auth_string = "--user {{params.username}}:{{params.password}}"

        super(DownloadFromJSONMetadataOperator, self).__init__(
            trigger_rule=trigger_rule,
            bash_command="""
                METADATA_FILE="""+metadata_file_filepath+""" &&
                OUT_PATH="""+downloaded_filepath+""" &&
                UUID=`python3 -c "import json; f=open('$METADATA_FILE'); """
            """print(json.load(f)[0]['uuid']); f.close()"` &&
                FILE_URL='https://scihub.copernicus.eu/s3/odata/v1/Products('\\'"$UUID"\\'')/$value'
                curl """+auth_string+""" -f $FILE_URL -o $OUT_PATH &&
                [[ -s $OUT_PATH ]]
            """,
            params={
                "username": username,
                "password": password,
            },
            **kwargs
        )
