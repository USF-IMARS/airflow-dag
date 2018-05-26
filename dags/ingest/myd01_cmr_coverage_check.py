from datetime import datetime

from airflow import DAG

from imars_dags.dags.ingest.cmr import coverage_check
from imars_dags.regions import gom
from imars_dags.util.globals import DEFAULT_ARGS
from imars_dags.util.get_dag_id import get_dag_id

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2002, 7, 4, 0, 0),
    'retries': 1
})

this_dag = DAG(
    dag_id=get_dag_id(
        __file__,
        region='gom',
        dag_name="myd01_cmr_coverage_check"
    ),
    default_args=default_args,
    schedule_interval=coverage_check.schedule_interval,
    catchup=False,
    max_active_runs=1
)

coverage_check.add_tasks(
    this_dag,
    region=gom,
    ingest_callback_dag_id="gom_modis_aqua_granule"
)
