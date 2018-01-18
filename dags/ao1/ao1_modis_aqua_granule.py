import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.dags.builders import modis_aqua_granule
from imars_dags.regions import ao1
from imars_dags.util.globals import QUEUE
from imars_dags.util import satfilename

this_dag = DAG(
    dag_id="ao1_modis_aqua_granule",
    default_args=modis_aqua_granule.default_args,
    schedule_interval=modis_aqua_granule.schedule_interval
)

modis_aqua_granule.add_tasks(
    this_dag,
    region=ao1,
    parfile=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/ao1/
        "moda_l2gen.par"
    )
)

create_granule_l3 = BashOperator(
    task_id="l3gen_granule",
    bash_command="""
        /opt/snap/bin/gpt {{params.gpt_xml_file}} \
        -t {{ params.satfilename.l3_pass(execution_date, params.roi_place_name) }} \
        -f NetCDF-BEAM \
        `{{ params.satfilename.l2(execution_date, params.roi_place_name) }}`
    """,
    params={
        'satfilename': satfilename,
        'roi_place_name': ao1.place_name,
        'gpt_xml_file': os.path.join(
            os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/ao1/
            "moda_l3g.par"
        )
    },
    queue=QUEUE.SNAP,
    dag = this_dag
)

# TODO: export tiff from l3
