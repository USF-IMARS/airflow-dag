import os

from airflow import DAG

from imars_dags.dags.builders import modis_aqua_granule
from imars_dags.regions import fgbnms

this_dag = DAG(
    dag_id="fgbnms_modis_aqua_granule",
    default_args=modis_aqua_granule.default_args,
    schedule_interval=modis_aqua_granule.schedule_interval
)

modis_aqua_granule.add_tasks(
    this_dag,
    region=fgbnms,
    parfile=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/fgbnms/
        "moda_l2gen.par"
    )
)
