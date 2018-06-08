"""
Reads the download url from a metadata .ini file created previously
(by CMRCoverageBranchOperator) and downloads the file.
"""

from airflow.operators.bash_operator import BashOperator

class DownloadFromMetadataFileOperator(BashOperator):
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

        super(DownloadFromMetadataFileOperator, self).__init__(
            trigger_rule=trigger_rule,
            bash_command="""
                METADATA_FILE="""+metadata_file_filepath+""" &&
                OUT_PATH="""+downloaded_filepath+""" &&
                FILE_URL=$(grep "^upstream_download_link" $METADATA_FILE | cut -d'=' -f2-) &&
                curl """+auth_string+""" -f $FILE_URL -o $OUT_PATH &&
                [[ -s $OUT_PATH ]]
            """,
            params={
                "username": username,
                "password": password,
            },
            **kwargs
        )
