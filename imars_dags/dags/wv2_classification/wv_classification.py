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
from imars_dags.dags.util.Area import Area
from imars_dags.util.globals import QUEUE

DAG_NAME = os.path.splitext(os.path.basename(__file__))[0]

AREAS = [
    'big_bend',
    'fl_se',
    'fl_ne',
    'monroe',
    'panhandle',
    'west_fl_pen',
    # 'tx_coast',  TODO: this one out-of-date
    'alabama',
    'mississippi',
    'la_east',
    'la_west',
    'texas_sw',
    'texas_ne',
    'texas_central'
]

for area_short_name in AREAS:
    area = Area(area_short_name)
    area_short_name = area.short_name
    area_id = area.id
    this_dag = DAG(
        dag_id=DAG_NAME + '_' + area_short_name,
        default_args={
            "start_date": datetime(2007, 9, 18)  # WV1 launch
        },
        schedule_interval=None,
        concurrency=20,
    )
    this_dag.doc_md = __doc__

    PRODUCT_ID_Rrs = 37
    ntf_to_rrs = BashOperator(  # noqa F841
        dag=this_dag,
        task_id='ntf_to_rrs',
        bash_command='scripts/ntf_to_rrs.sh',
        params={  # TODO: clean up unused params here
            # product ids from metadata db
            "Rrs_ID": PRODUCT_ID_Rrs,
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

    # TODO: rrs_to_class

    # === DAG connections
    # ntf_to_rrs >> rrs_to_class

    # === add the dag to globals with unique name so airflow can find it
    globals()[this_dag.dag_id] = this_dag
