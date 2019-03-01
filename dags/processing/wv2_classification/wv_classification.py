"""
classification on WorldView-2 images

# old circe stuff:
# #SBATCH --job-name ="wv2_classification_py"
# #SBATCH --ntasks=1
# #SBATCH --mem-per-cpu=20480
# #SBATCH --time=3:00:00
# #SBATCH --array=0-3
"""
# std libs
from datetime import datetime
import os

# deps
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# this package
from imars_dags.util.get_default_args import get_default_args
from imars_dags.util.globals import QUEUE
from imars_dags.util.get_dag_id import get_dag_id
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
    area_short_name = area.short_name
    area_id = area.id
    this_dag = DAG(
        dag_id=get_dag_id(
            __file__,
            region=area_short_name,
            dag_name=DAG_NAME
        ),
        default_args=get_default_args(
            start_date=datetime.utcnow()
        ),
        schedule_interval=None,
    )

    wv_classify = BashOperator(  # noqa F841
        dag=this_dag,
        task_id='wv_classify',
        bash_command='scripts/ntf_to_rrs.sh',
        params={
            # product ids from metadata db
            "Rrs_ID": 37,
            "rrs_ID": 38,
            "bth_ID": 39,
            "classf_ID": 40,
            # algorithm settings
            "id": 0,
            "crd_sys": "EPSG:4326",
            "dt": 0,
            "sgw": "5",
            "filt": 0,
            "stat": 3,
            "loc": 'testnew',
            "id_number": 0,  # (prev SLURM_ARRAY_TASK_ID) TODO: rm this?
            # area information for extract & load
            "area_id": area_id
        },
        queue=QUEUE.WV2_PROC,
    )

    # must add the dag to globals with unique name so
    # airflow can find it
    globals()[this_dag.dag_id] = this_dag
