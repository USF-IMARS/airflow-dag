"""
This dag periodically ingests files output from dotis's cronjobs into the
IMaRS product metadata db.
"""
# unused airflow.DAG import is so airflow can find this dag.
from airflow import DAG  # noqa F401

from imars_dags.dag_classes.IngestDirectoryDAG import IngestDirectoryDAG
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.merge_dicts import merge_dicts

COMMON_ARGS = {
    'duplicates_ok': True,  # don't freak out over duplicates
    'nohash': True,  # speeds things up a lot
    # leave files alone
    'object_store': 'no_upload',
    # 'dry_run': True,  # True if we are just testing
    'area_id': 2,  # from metaDB must match area_short_name
    'area_short_name': 'fgbnms',  # optional?
}

dotis_cronjob_outputs_chlor_a = IngestDirectoryDAG(
    directory_path='/srv/imars-objects/modis_aqua_fgbnms/png_chl_7d/',
    dag_id=get_dag_id(__file__, dag_name="dotis_cronjob_outputs_chlor_a"),
    load_kwargs_list=[
        merge_dicts(
            {
                'product_id': 43,  # from metaDB
                'product_type_name': 'a1km_chlor_a_7d_mean_png',  # optional
            },
            COMMON_ARGS
        ),
        merge_dicts(
            {
                'product_id': 44,  # from metaDB
                'product_type_name': 'a1km_chlor_a_7d_anom_png',  # optional
            },
            COMMON_ARGS
        )
    ]
)

# TODO: these DAGs should be in different files
dotis_cronjob_outputs_sst = IngestDirectoryDAG(
    directory_path='/srv/imars-objects/modis_aqua_fgbnms/PNG_AQUA/SST/',
    dag_id=get_dag_id(__file__, dag_name="dotis_cronjob_outputs_sst"),
    load_kwargs_list=[
        merge_dicts(
            {
                'product_id': 45,  # from metaDB
                'product_type_name': 'a1km_sst_7d_mean_png',  # optional
            },
            COMMON_ARGS
        ),
        merge_dicts(
            {
                'product_id': 46,  # from metaDB
                'product_type_name': 'a1km_sst_7d_anom_png',  # optional
            },
            COMMON_ARGS
        )
    ]
)
