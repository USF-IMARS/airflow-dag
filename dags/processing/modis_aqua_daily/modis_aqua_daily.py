"""
airflow processing pipeline definition for MODIS aqua daily processing
"""
import os
from datetime import datetime, timedelta

from airflow import DAG

from imars_dags.util.globals import DEFAULT_ARGS
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.dags.processing.modis_aqua_daily.add_l3gen import add_l3gen
from imars_dags.dags.processing.modis_aqua_daily.add_png_exports \
    import add_png_exports

default_args = DEFAULT_ARGS.copy()
# NOTE: start_date must be 12:00 (see _wait_for_passes_subdag)
default_args.update({
    'start_date': datetime(2018, 1, 8, 0, 0),
})

AREA_SHORT_NAME = "gom"

this_dag = DAG(
    dag_id=get_dag_id(__file__, region=AREA_SHORT_NAME),
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

add_l3gen(
    this_dag,
    region_name=AREA_SHORT_NAME,
    gpt_xml=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "L3G_MODA_GOM_v2.xml"
    )
)

add_png_exports(
    this_dag,
    region_name=AREA_SHORT_NAME,
    variable_names=['chlor_a', 'nflh']
)
