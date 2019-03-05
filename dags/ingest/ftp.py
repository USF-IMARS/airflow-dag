"""
DAG to define the FTP ingest process.

Files are uploaded to the central IMaRS FTP server then this runs and sorts out
where things should go.
"""
# from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.dag_classes.IngestDirectoryDAG import IngestDirectoryDAG
from imars_dags.util.get_dag_id import get_dag_id


this_dag = IngestDirectoryDAG(
    directory_path='/srv/imars-objects/ftp-ingest/complete/',
    dag_id=get_dag_id(__file__, "na"),
    load_kwargs_list=[
        {
            'product_id': 6,  # from metaDB zip_wv2_ftp_ingest
        },
        {
            'product_id': 47,  # from metaDB zip_wv3_ftp_ingest
        },
    ],
    schedule_interval="2 22 * * 0",
    rm_loaded=True,
    concurrency=1,
)

with this_dag as dag:
    # new way of doing it: just use bashOp & imars-etl:
    ingest_s3a_ol_1_efr = BashOperator(
        task_id="ingest_s3a_ol_1_efr",
        bash_command="""\
            find /srv/imars-objects/ftp-ingest/fl_sen3/ \
                -type f \
                -name "S3A_OL_1_EFR___*.zip" |
            xargs -n 1 -I % sh -c ' \
                imars-etl load \
                    --product_id 36 \
                    --sql " \
                        status_id=3 AND
                        area_id=12 AND
                        provenance='af-ftp_v1' \
                    " \
                    --slstr path_format \
                    --duplicates_ok \
                    --nohash \
                    % &&
                mv % /srv/imars-objects/trash/.
            '
        """
    )
