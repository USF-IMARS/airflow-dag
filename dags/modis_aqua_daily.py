"""
airflow processing pipeline definition for MODIS aqua daily processing
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.sensors import TimeDeltaSensor
from airflow.utils.state import State
from datetime import timedelta, datetime

# === ./imars_dags/modis_aqua_processing.py :
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, SLEEP_ARGS
from imars_dags.util import satfilename
from imars_dags.settings.regions import REGIONS
from imars_dags.settings.png_export_transforms import png_export_transforms
from imars_dags.operators.l3gen import get_l3gen
from imars_dags.operators.wait_for_all_day_granules_checked \
    import get_wait_for_all_day_granules_checked
from imars_dags.operators.wait_for_pass_processing_success \
    import get_wait_for_pass_processing_success

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
        max_active_runs=1  # this must be limited b/c the subdag spawns 288 tasks,
                           # which can easily lead to deadlocks.
    ) as dag:
        # =========================================================================
        # === delay to wait for day to end, so all passes that day are done.
        # =========================================================================
        wait_for_day_end = TimeDeltaSensor(
            delta=timedelta(hours=18),  # 12 hrs to midnight + 6 hrs just in case
            task_id='wait_for_data_delay',
            **SLEEP_ARGS
        )
        # =========================================================================
        l3gen = get_l3gen(region)
        # =========================================================================
        # === wait for pass-level processing
        # =========================================================================
        wait_for_all_day_granules_checked = get_wait_for_all_day_granules_checked()
        wait_for_day_end >> wait_for_all_day_granules_checked >> l3gen

        wait_for_pass_processing_success = get_wait_for_pass_processing_success(region)
        wait_for_day_end >> wait_for_pass_processing_success >> l3gen
        # =========================================================================
        # =========================================================================
        # === export png(s) from l3 netCDF4 file
        # =========================================================================
        for variable_name in region['png_exports']:
            try:
                var_transform = png_export_transforms[variable_name]
            except KeyError as k_err:
                # no transform found, passing data through w/o scaling
                # NOTE: not recommended. data is expected to be range [0,255]
                var_transform = "data"
            l3_to_png = BashOperator(
                task_id="l3_to_png_"+variable_name,
                bash_command="""
                /opt/sat-scripts/sat-scripts/netcdf4_to_png.py \
                {{params.satfilename.l3(execution_date, params.roi_place_name)}} \
                {{params.satfilename.png(execution_date, params.variable_name, params.roi_place_name)}} \
                {{params.variable_name}}\
                -t '{{params.transform}}'
                """,
                params={
                    'satfilename': satfilename,
                    'variable_name': variable_name,
                    'transform': var_transform,
                    'roi_place_name': region['place_name']
                },
                queue=QUEUE.SAT_SCRIPTS
            )
            l3gen >> l3_to_png
        # =========================================================================
        return dag
