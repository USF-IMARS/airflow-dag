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
from airflow.operators.python_operator import PythonOperator

# this package
from imars_dags.util.get_default_args import get_default_args
from imars_dags.util.globals import QUEUE
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.Area import Area
from imars_dags.dags.processing.wv2_classification.scripts.add_img_points_to_csv \
    import add_img_points_to_csv


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
            start_date=datetime(2007, 9, 18)  # WV1 launch
        ),
        schedule_interval=None,
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

    # === save band values from selected points for later analysis
    # TODO: all points currently in west fl pen,
    #       but hopefully we get more and rm this if.
    if area_short_name == 'west_fl_pen':
        img_bands_at_pts_to_csv = PythonOperator(
            dag=this_dag,
            task_id='img_bands_at_pts_to_csv',
            queue=QUEUE.WV2_PROC,
            provide_context=True,
            python_callable=add_img_points_to_csv,
            op_kwargs={
                'product_id': PRODUCT_ID_Rrs
            }
        )
        ntf_to_rrs >> img_bands_at_pts_to_csv

    # === DAG connections
    # ntf_to_rrs >> rrs_to_class

    # === add the dag to globals with unique name so airflow can find it
    globals()[this_dag.dag_id] = this_dag
