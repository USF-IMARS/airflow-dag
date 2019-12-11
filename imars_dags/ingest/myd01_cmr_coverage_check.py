"""
Checks NASA CMR for daytime modis aqua passes over the GoM region.
"""
from datetime import timedelta

# unused DAG import is required for airflow to find the dag
from airflow import DAG  # noqa:F401
from imars_dags.dag_classes.ingest.cmr.CMRCoverageCheckDAG \
    import CMRCoverageCheckDAG
from imars_dags.regions import gom

this_dag = CMRCoverageCheckDAG(
    region=gom,
    region_short_name='gom',
    region_id=1,

    product_short_name='myd01',
    product_id=5,

    cmr_search_kwargs={
        'short_name': "MYD01",    # [M]odis (Y)aqua (D) (0) level [1]
        'day_night_flag': 'day',  # day only for ocean color
    },
    granule_len=timedelta(minutes=5),
    check_delay=timedelta(hours=3)
)
this_dag.doc_md = __doc__
