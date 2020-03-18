"""
This dag periodically ingests files output from dotis's cronjobs into the
IMaRS product metadata db.
"""
# unused airflow.DAG import is so airflow can find this dag.
from airflow import DAG  # noqa F401

from imars_dags.dag_classes.IngestDirectoryDAG import IngestDirectoryDAG
from imars_dags.util.merge_dicts import merge_dicts

COMMON_ARGS = {
    # leave files alone
    'object_store': 'no_upload',
    'area_id': 2,  # from metaDB must match area_short_name
    'area_short_name': 'fgbnms',  # optional?
}

dotis_cronjob_outputs_chlor_a = IngestDirectoryDAG(
    directory_path='/srv/imars-objects/modis_aqua_fgbnms/png_chl_7d/',
    dag_id="dotis_cronjob_outputs_chlor_a",
    load_kwargs_list=[
        merge_dicts(
            COMMON_ARGS,
            {
                'product_id': 43,  # from metaDB
                'product_type_name': 'a1km_chlor_a_7d_mean_png',  # optional
            }
        ),
        merge_dicts(
            COMMON_ARGS,
            {
                'product_id': 44,  # from metaDB
                'product_type_name': 'a1km_chlor_a_7d_anom_png',  # optional
            }
        )
    ]
)
dotis_cronjob_outputs_chlor_a.doc_md = __doc__

# TODO: these DAGs should be in different files
dotis_cronjob_outputs_sst = IngestDirectoryDAG(
    directory_path='/srv/imars-objects/modis_aqua_fgbnms/PNG_AQUA/SST/',
    dag_id="dotis_cronjob_outputs_sst",
    load_kwargs_list=[
        merge_dicts(
            COMMON_ARGS,
            {
                'product_id': 45,  # from metaDB
                'product_type_name': 'a1km_sst_7d_mean_png',  # optional
            }
        ),
        merge_dicts(
            COMMON_ARGS,
            {
                'product_id': 46,  # from metaDB
                'product_type_name': 'a1km_sst_7d_anom_png',  # optional
            }
        )
    ]
)
dotis_cronjob_outputs_sst.doc_md = __doc__
