"""
airflow processing pipeline definition for MODIS aqua daily processing
"""
from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.sensors import TimeDeltaSensor
from airflow.utils.state import State
from datetime import timedelta, datetime

# === ./imars_dags/modis_aqua_processing.py :
from imars_dags.util.globals import DEFAULT_ARGS, SLEEP_ARGS
from imars_dags.util import satfilename
from imars_dags.settings.regions import REGIONS
from imars_dags.operators.L3Gen import L3Gen
from imars_dags.operators.wait_for_all_day_granules_checked \
    import get_wait_for_all_day_granules_checked
from imars_dags.operators.wait_for_pass_processing_success \
    import get_wait_for_pass_processing_success
from imars_dags.operators.l3_to_png import get_l3_to_png

def get_modis_aqua_daily_dag(region):
    default_args = DEFAULT_ARGS.copy()
    # NOTE: start_date must be 12:00 (see _wait_for_passes_subdag)
    default_args.update({
        'start_date': datetime(2018, 1, 3, 12, 0),
        'retries': 1
    })
    with DAG(
        'modis_aqua_daily_' + region['place_name'],
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        max_active_runs=1
    ) as dag:
        # === delay to wait for day to end, so all passes that day are done.
        wait_for_day_end = TimeDeltaSensor(
            delta=timedelta(hours=18),  # 12 hrs to midnight + 6 hrs just b/c
            task_id='wait_for_data_delay',
            **SLEEP_ARGS
        )

        l3gen = L3Gen(region)

        # === wait for pass-level processing
        wait_for_all_day_granules_checked = get_wait_for_all_day_granules_checked()
        wait_for_day_end >> wait_for_all_day_granules_checked >> l3gen

        wait_for_pass_processing_success = get_wait_for_pass_processing_success(region)
        wait_for_day_end >> wait_for_pass_processing_success >> l3gen

        # === export png(s) from l3 netCDF4 file
        for variable_name in region['png_exports']:
            l3_to_png = get_l3_to_png(variable_name, region)
            l3gen >> l3_to_png
        return dag
