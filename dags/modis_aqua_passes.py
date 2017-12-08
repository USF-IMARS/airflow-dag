"""
airflow processing pipeline definition for MODIS aqua per-pass processing
"""
# std libs
from datetime import timedelta

# deps
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import TimeDeltaSensor
from pyCMR.pyCMR import CMR

# this package
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, POOL
from imars_dags.util import satfilename
from imars_dags.settings.regions import REGIONS


# one DAG for each pass
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
# the metadata being available in the CMR. Usually something like 2-48 hours.
wait_for_data_delay = TimeDeltaSensor(
    delta=timedelta(hours=2),
    task_id='wait_for_data_delay',
    dag=this_dag
)
# =============================================================================
# =============================================================================
# === check if this granule covers our ROIs using metadata from CMR
# =============================================================================
def granule_in_roi(exec_datetime):
    """
    returns true if granule for given datetime is in one of our ROIs

    NOTE: we get the granule metadata *without* server-side ROI check
    & do ROI check locally instead so we can be sure that the data
    has published. We want this to fail if we can't find the metadata, else
    we could end thinking granules are not in our ROI when actually they may
    just be late to publish.
    """
    cmr=CMR("../cmr.cfg")
    time_range = str(
        exec_datetime.strftime(iso8601) + ',' +
        (exec_datetime + timedelta(minutes=4, seconds=59)).strftime(iso8601)
    )
    results = cmr.searchGranule(
        limit=10,
        short_name="MOD09CMG",
        temporal=time_range
    )
    assert len(results) == 1

    # TODO: check if bounding box in res intersects with any of our ROIs
    return False

def decide_which_path():
    if granule_in_roi(exec_datetime) is True:
        return "download_granule"
    else:
        return "skip_granule"

metadata_check = BranchPythonOperator(
    task_id='metadata_check',
    python_callable=(decide_which_path),
    trigger_rule="all_done",
    dag=dag
)

metadata_check >> download_granule
metadata_check >> skip_granule
# =============================================================================
# =============================================================================
# === do nothing on this granule, just end the DAG
# =============================================================================
skip_granule = DummyOperator(
    task_id='skip_granule',
    trigger_rule='one_success',
    dag=dag
)
# =============================================================================
# =============================================================================
# === Download the granule
# =============================================================================
# TODO: make this actually do stuff (dl w/ pycmr?)
download_granule = DummyOperator(
    task_id='download_granule',
    trigger_rule='one_success',
    dag=dag
)

# =============================================================================
# TODO: continue processing here...
