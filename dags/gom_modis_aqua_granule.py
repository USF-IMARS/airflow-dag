from airflow import DAG

from imars_dags.dags.builders import modis_aqua_granule
from imars_dags.regions import gom

this_dag = DAG(
    dag_id="modis_aqua_granule_gom",
    default_args=modis_aqua_granule.default_args,
    schedule_interval=modis_aqua_granule.schedule_interval
)

modis_aqua_granule.add_tasks(
    this_dag,
    region=gom
)
