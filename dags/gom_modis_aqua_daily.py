from datetime import datetime

from airflow import DAG

from imars_dags.util.globals import DEFAULT_ARGS
from imars_dags.dags.builders import modis_aqua_daily
from imars_dags.regions import gom

default_args = DEFAULT_ARGS.copy()
# NOTE: start_date must be 12:00 (see _wait_for_passes_subdag)
default_args.update({
    'start_date': datetime(2018, 1, 3, 12, 0),
    'retries': 1
})

this_dag = DAG(
    dag_id="gom_modis_aqua_daily",
    default_args=default_args,
    schedule_interval=modis_aqua_daily.schedule_interval
)

modis_aqua_daily.add_tasks(
    this_dag,
    region=gom
)
modis_aqua_daily.add_png_exports(
    this_dag,
    region=gom,
    variable_names=['chlor_a', 'nflh']
)
