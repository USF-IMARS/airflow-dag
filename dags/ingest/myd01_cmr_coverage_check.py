from datetime import timedelta

from airflow import DAG
from imars_dags.dags.ingest.cmr.CMRCoverageCheckDAG import CMRCoverageCheckDAG
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
