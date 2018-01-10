import os

from airflow import DAG

from imars_dags.dags.builders import modis_aqua_granule
from imars_dags.regions import gom

this_dag = DAG(
    dag_id="gom_modis_aqua_granule",
    default_args=modis_aqua_granule.default_args,
    schedule_interval=modis_aqua_granule.schedule_interval
)

modis_aqua_granule.add_tasks(
    this_dag,
    region=gom,
    parfile=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/gom/
        "moda_l3g.par"
    )
)
