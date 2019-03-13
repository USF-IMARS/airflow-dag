"""
=========================================================================
wv2 unzip to final destination
=========================================================================
"""
from datetime import datetime
from datetime import timedelta
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.get_default_args import get_default_args
from imars_dags.util.Area import Area

DAG_NAME = os.path.splitext(os.path.basename(__file__))[0]

AREAS = [
    'big_bend',
    'fl_se',
    'fl_ne',
    'monroe',
    'panhandle',
    'west_fl_pen',
    'tx_coast',
]

for area_short_name in AREAS:
    area = Area(area_short_name)
    AREA_SHORT_NAME = area.short_name
    AREA_ID = area.id
    DAG_ID = get_dag_id(
        __file__, region=AREA_SHORT_NAME, dag_name=DAG_NAME
    )
    this_dag = DAG(
        dag_id=DAG_ID,
        default_args=get_default_args(
            start_date=datetime(1980, 1, 1),
            retry_delay=timedelta(minutes=3)
        ),
        schedule_interval=None,
    )
    this_dag.doc_md = __doc__

    COMMON_LOAD_ARGS = {  # args common to all outputs
        # example file: "./058655513010_01_003/058655513010_01/
        #               058655513010_01_P001_MUL/
        #               18JUL16161240-M1BS-058655513010_01_P001.ATT"
        # example xml : "./058655513010_01_003/058655513010_01/
        #               058655513010_01_P001_MUL/
        #               18JUL16161240-M1BS-058655513010_01_P001.XML"
    }
    unzip_wv2_ingest = BashOperator(
        task_id="unzip_wv2_ingest",
        dag=this_dag,
        bash_command="wv2_unzip.sh",
        params=dict(
            area_id=AREA_ID
        ),
        task_concurrency=2  # TODO: increase this as # workers increases
    )

    # must add the dag to globals with unique name so airflow can find it
    globals()[DAG_ID] = this_dag
