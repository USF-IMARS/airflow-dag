"""
airflow processing pipeline definition for MODIS aqua daily processing
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import TimeDeltaSensor
from datetime import timedelta, datetime

# === ./imars_dags/modis_aqua_processing.py :
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, POOL
from imars_dags.util import satfilename
from imars_dags.settings.regions import REGIONS

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2017, 12, 10, 0, 0),
    'retries': 1
})
this_dag = DAG(
    'modis_aqua_daily',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

def get_todays_l2s(todays_datetime):
    """ returns a list of l2 files """
    return []
# TODO: query earthdata CMR to find what granules cover this day,
# (for info see https://github.com/USF-IMARS/imars_dags/issues/2 )
# then spin up an ExternalTaskSensor for each pass so that we wait for the
# pass-level processing to complete before continuing.
# [ref](https://stackoverflow.com/a/38028511/1483986)
# NOTE: we might be able to remove the delay below

# =============================================================================
# === delay to wait for upstream data to become available.
# =============================================================================
# ie wait for generation of the full day's l2s to complete.
# this makes sure that we don't try to run this DAG until `delta` amount of time
# past the `execution_date` (which is the datetime of the satellite recording).
#
# `delta` is the amount of time we expect between satellite measurement and
# the data being available on our server. Usually something like 48 hours.
wait_for_data_delay = TimeDeltaSensor(
    delta=timedelta(days=2),
    task_id='wait_for_data_delay',
    dag=this_dag
)
# =============================================================================

# =============================================================================
# === L3 Generation using GPT graph
# =============================================================================
# this assumes the l2 files for the whole day have already been generated
#
# example cmd:
#     /opt/snap/5.0.0/bin/gpt L3G_MODA_GOM_vIMARS.xml
#     -t /home1/scratch/epa/satellite/modis/GOM/L3G_OC/A2017313_map.nc
#     -f NetCDF-BEAM
#     /srv/imars-objects/modis_aqua_gom/l2/A2017313174500.L2
#     /srv/imars-objects/modis_aqua_gom/l2/A2017313192000.L2
#     /srv/imars-objects/modis_aqua_gom/l2/A2017313192500.L2
#
#     -t is the target (output) file, -f is the format

l3gen = BashOperator(
    task_id="l3gen",
    bash_command="""
        /opt/snap/5.0.0/bin/gpt /root/airflow/dags/settings/L3G_MODA_GOM_vIMARS.xml
        -t {{ params.satfilename.l3(execution_date) }}
        -f NetCDF-BEAM
        {{ params.get_todays_l2s(execution_date) }}
    """,
    params={
        'satfilename': satfilename,
        'get_todays_l2s':get_todays_l2s
    },
    dag=this_dag
)
# =============================================================================
