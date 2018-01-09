"""
airflow processing pipeline definition for MODIS aqua daily processing
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.sensors import TimeDeltaSensor, SqlSensor
from airflow.utils.state import State
from datetime import timedelta, datetime

# === ./imars_dags/modis_aqua_processing.py :
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, SLEEP_ARGS
from imars_dags.util import satfilename
from imars_dags.settings.regions import REGIONS
from imars_dags.settings.png_export_transforms import png_export_transforms
from imars_dags.operators.l3gen import get_l3gen

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
        # === wait for every granule to be checked for coverage
        # Passes when # of "success" controller dags for today >= 288
        # ie, when the dag has run for every 5min granule today.
        wait_for_all_day_granules_checked = SqlSensor(
            task_id='wait_for_all_day_granules_checked',
            conn_id='mysql_default',
            sql="""
            SELECT GREATEST(COUNT(state)-287, 0)
                FROM dag_run WHERE
                    (execution_date BETWEEN
                        '{{execution_date.replace(hour=0,minute=0)}}' AND '{{execution_date.replace(hour=23,minute=59)}}')
                    AND dag_id='modis_aqua_passes_controller'
                    AND state='success';
            """
        )
        wait_for_day_end >> wait_for_all_day_ganules_checked >> l3gen

        # === wait for granules that were covered to finish processing.
        # Here we use an SqlSensor to check the metadata db instead of trying
        # to generate a dynamic list of ExternalTaskSensors.
        wait_for_pass_processing_success = SqlSensor(
            task_id='wait_for_pass_processing_success',
            conn_id='mysql_default',
            sql="""
                SELECT 1 - LEAST(COUNT(state),1)
                    FROM dag_run WHERE
                        (execution_date BETWEEN
                            '{{execution_date.replace(hour=0,minute=0)}}' AND '{{execution_date.replace(hour=23,minute=59)}}')
                        AND dag_id='modis_aqua_pass_processing_'"""+region['place_name']+"""
                        AND state!='success'
                ;
                """
        )
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
