"""
DAG to define the FTP ingest process.

Files are uploaded to the central IMaRS FTP server then this runs and sorts out
where things should go.
"""
# from airflow import DAG

from imars_dags.dag_classes.IngestDirectoryDAG import IngestDirectoryDAG
from imars_dags.util.get_dag_id import get_dag_id


this_dag = IngestDirectoryDAG(
    directory_path='/srv/imars-objects/ftp-ingest/',
    dag_id=get_dag_id(__file__, "na"),
    load_kwargs_list=[
        {
            'product_id': 6,  # from metaDB
        },
        {
            'product_id': 47,  # from metaDB
        },
    ],
    schedule_interval="2 22 * * 0",
    rm_loaded=True,
)
