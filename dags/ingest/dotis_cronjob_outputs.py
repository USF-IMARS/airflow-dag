# unused airflow.DAG import is so airflow can find this dag.
from airflow import DAG  # noqa F401

from imars_dags.dag_classes.IngestDirectoryDAG import IngestDirectoryDAG


dotis_cronjob_outputs = IngestDirectoryDAG(
    directory_to_watch='/srv/imars-objects/modis_aqua_fgbnms/png_chl_7d/',
    etl_load_args={
        'product_id': 43,  # from metaDB
        'product_type_name': 'a1km_chlor_a_7d_mean_png',  # optional
        'area_id': 2,  # from metaDB must match area_short_name
        'area_short_name': 'fgbnms',  # optional?
        'status_id': 3,
        'duplicates_ok': True,
        'nohash': True,  # speeds things up a lot
        'storage_driver': 'no_upload',  # leave the files where they are
        'dry_run': True,  # True if we are just testing
    },
)
