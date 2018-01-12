from datetime import datetime

from airflow import DAG

from imars_dags.dags.builders import modis_aqua_coverage_check
from imars_dags.regions import fgbnms
from imars_dags.util.globals import DEFAULT_ARGS

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2002, 7, 4, 0, 0),
    'retries': 1
})

this_dag = DAG(
    dag_id="fgbnms_modis_aqua_coverage_check",
    default_args=default_args,
    schedule_interval=modis_aqua_coverage_check.schedule_interval
)

modis_aqua_coverage_check.add_tasks(
    this_dag,
    region=fgbnms,
    process_pass_dag_name="fgbnms_modis_aqua_granule"
)
