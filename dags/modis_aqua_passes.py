"""
airflow processing pipeline definition for MODIS aqua per-pass processing
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import TimeDeltaSensor
from datetime import timedelta

from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, POOL
from imars_dags.util import satfilename
from imars_dags.settings.regions import REGIONS

# for each (new) pass file:
this_dag = DAG(
    'modis_aqua_passes',
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(minutes=5)
)

# =============================================================================
# === delay to wait for upstream data to become available.
# =============================================================================
# ie wait for download from  OB.DAAC to complete.
# this makes sure that we don't try to run this DAG until `delta` amount of time
# past the `execution_date` (which is the datetime of the satellite recording).
#
# `delta` is the amount of time we expect between satellite measurement and
# the data being available on our server. Usually something like 2-48 hours.
wait_for_data_delay = TimeDeltaSensor(
    delta=timedelta(hours=36),
    task_id='wait_for_data_delay',
    dag=this_dag
)
# =============================================================================
# === get metadata for this granule from CMR
# =============================================================================
# =============================================================================
get_metadata = BashOperator(
    task_id='get_metadata',
    bash_command="""
    wget
    -o satpath.pass_metadata(execution_date)
    get_cmr_pass_url(execution_date)
    """
    dag=this_dag
    params={
        "satpath": satfilename,  # TODO: implement pass_metadata()
        "get_cmr_pass_url": get_cmr_pass_url  # TODO: implement url-maker https://cmr.earthdata.nasa.gov/search/granules.json?short_name=MOD09CMG&page_num=1&page_size=50&temporal=2010-02-01T10%3A00%3A00Z%2C2010-02-01T12%3A00%3A00Z
    }
)
# =============================================================================

# TODO: if metadata says the granule is in our ROI, start processing, else rm metadata file
