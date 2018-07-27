# unused airflow.DAG import is so airflow can find this dag.
from airflow import DAG  # noqa F401

from imars_dags.dag_classes.IngestDirectoryDAG import IngestDirectoryDAG
from imars_dags.util.get_dag_id import get_dag_id


dotis_cronjob_outputs = IngestDirectoryDAG(
    dag_id=get_dag_id(__file__, dag_name="dotis_cronjob_outputs")
)

dotis_cronjob_outputs.add_ingest_task(
    task_id='a1km_chlor_a_7d_mean_png',
    etl_load_args={
        'directory': '/srv/imars-objects/modis_aqua_fgbnms/png_chl_7d/',
        'product_id': 43,  # from metaDB
        'product_type_name': 'a1km_chlor_a_7d_mean_png',  # optional
        'area_id': 2,  # from metaDB must match area_short_name
        'area_short_name': 'fgbnms',  # optional?
        'duplicates_ok': True,  # don't freak out over duplicates
        'nohash': True,  # speeds things up a lot
        'storage_driver': 'no_upload',  # leave the files where they are
        'dry_run': True,  # True if we are just testing
    },
)
dotis_cronjob_outputs.add_ingest_task(
    task_id='a1km_chlor_a_7d_anom_png',
    etl_load_args={
        'directory': '/srv/imars-objects/modis_aqua_fgbnms/png_chl_7d/',
        'product_id': 44,  # from metaDB
        'product_type_name': 'a1km_chlor_a_7d_anom_png',  # optional
        'area_id': 2,  # from metaDB must match area_short_name
        'area_short_name': 'fgbnms',  # optional?
        'duplicates_ok': True,  # don't freak out over duplicates
        'nohash': True,  # speeds things up a lot
        'storage_driver': 'no_upload',  # leave the files where they are
        'dry_run': True,  # True if we are just testing
    },
)
dotis_cronjob_outputs.add_ingest_task(
    task_id='a1km_sst_7d_mean_png',
    etl_load_args={
        'directory':
            '/srv/imars-objects/modis_aqua_fgbnms/png_chl_7d/PNG_AQUA/SST/',
        'product_id': 45,  # from metaDB
        'product_type_name': 'a1km_sst_7d_mean_png',  # optional
        'area_id': 2,  # from metaDB must match area_short_name
        'area_short_name': 'fgbnms',  # optional?
        'duplicates_ok': True,  # don't freak out over duplicates
        'nohash': True,  # speeds things up a lot
        'storage_driver': 'no_upload',  # leave the files where they are
        'dry_run': True,  # True if we are just testing
    },
)
dotis_cronjob_outputs.add_ingest_task(
    task_id='a1km_sst_7d_anom_png',
    etl_load_args={
        'directory':
            '/srv/imars-objects/modis_aqua_fgbnms/png_chl_7d/PNG_AQUA/SST/',
        'product_id': 46,  # from metaDB
        'product_type_name': 'a1km_sst_7d_anom_png',  # optional
        'area_id': 2,  # from metaDB must match area_short_name
        'area_short_name': 'fgbnms',  # optional?
        'duplicates_ok': True,  # don't freak out over duplicates
        'nohash': True,  # speeds things up a lot
        'storage_driver': 'no_upload',  # leave the files where they are
        'dry_run': True,  # True if we are just testing
    },
)
