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

# deps
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# this package
from imars_dags.util.get_default_args import get_default_args
from imars_dags.util.globals import QUEUE
from imars_dags.util.get_dag_id import get_dag_id


def get_dag(area_short_name, area_id):
    this_dag = DAG(
        dag_id=get_dag_id(
            __file__,
            region=area_short_name,
            dag_name="wv_classification"
        ),
        default_args=get_default_args(
            start_date=datetime.utcnow()
        ),
        schedule_interval=None,
    )

    wv_classify = BashOperator(  # noqa F841
        dag=this_dag,
        task_id='wv_classify',
        bash_command='wv_classify.sh',
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

    return this_dag

na_dag = get_dag('na', 5)
big_bend_dag = get_dag('big_bend', 6)
fl_se_dag = get_dag('fl_se', 7)
fl_ne_dag = get_dag('fl_ne', 8)
monroe_dag = get_dag('monroe', 9)
panhandle_dag = get_dag('panhandle', 10)
west_fl_pen_dag = get_dag('west_fl_pen', 11)
