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
from imars_dags.settings import secrets  # NOTE: this file not in public repo!


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

    NOTE: we get the granule metadata *without* server-side ROI check first
    & then do ROI check so we can be sure that the data
    has published. We want this to fail if we can't find the metadata, else
    we could end up thinking granules are not in our ROI when actually they may
    just be late to publish.
    """
    # === set up basic query for CMR
    TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"  # iso 8601
    cmr = CMR("/root/airflow/dags/imars_dags/settings/cmr.cfg")
    time_range = str(
        (exec_datetime + timedelta(           seconds=1 )).strftime(TIME_FMT) + ',' +
        (exec_datetime + timedelta(minutes=4, seconds=59)).strftime(TIME_FMT)
    )
    search_kwargs={
        'limit':10,
        'short_name':"MYD01",  # [M]odis (Y)aqua (D) (0) level [1]
        # 'collection_data_type':"NRT",  # this is not available for granules
        # 'provider':"LANCEMODIS",  # lance modis is hopefullly only serving NRT
        'temporal':time_range
    }
    print(search_kwargs)
    # === initial metadata check
    results = cmr.searchGranule(**search_kwargs)
    print("results:")
    print(results)
    assert(len(results) > 0)

    # === check if bounding box in res intersects with any of our ROIs
    # we do this w/ another CMR call so we don't have to do the math.
    roi = REGIONS[0]  # we only have one region right now
    # re-use the original search_kwargs, but add bounding box
    search_kwargs['bounding_box']="{},{},{},{}".format(
        roi['lonmin'],  # low l long
        roi['latmin'],  # low l lat
        roi['lonmax'],  # up r long
        roi['latmax']   # up r lat
    )
    bounded_results = cmr.searchGranule(**search_kwargs)
    if (len(bounded_results) == 1):  # granule intersects our ROI
        return True
    elif (len(bounded_results) == 0):  # granule does not intersect our ROI
        return False
    else:
        raise ValueError("unexpected # of results from ROI CMR search:"+str(len(bounded_results)))

def _metadata_check(ds, **kwargs):
    """
    Performs metadata_check to decide which path the DAG should take.

    Parameters
    -----------
    ds : datetime?
        *I think* this is the execution_date for the operator instance
    kwargs['execution_date'] : datetime.datetime
        the execution_date for the operator instance (same as `ds`?)
    kwargs['ti'] : task-instance?
        something that we use to push/pull xcoms

    Returns
    -----------
    str
        name of the next operator that should trigger (ie the first in the
        branch that we want to follow)
    """
    if granule_in_roi(kwargs['execution_date']) is True:
        # TODO: use real dl url here
        kwargs['ti'].xcom_push(key='download_url', value="ftp://test.test?test=test&test")
        return "download_granule"
    else:
        return "skip_granule"

metadata_check = BranchPythonOperator(
    task_id='metadata_check',
    python_callable=_metadata_check,
    provide_context=True,
    # trigger_rule="all_success",
    dag=this_dag
)

# =============================================================================
# =============================================================================
# === do nothing on this granule, just end the DAG
# =============================================================================
skip_granule = DummyOperator(
    task_id='skip_granule',
    trigger_rule='one_success',
    dag=this_dag
)
metadata_check >> skip_granule

# =============================================================================
# =============================================================================
# === Download the granule
# =============================================================================
# TODO: make this actually do stuff
#   The href below can be pulled out of the CMR response (below) & d/l w/ wget,
#   or the CMR response itself can be passed here using an
#   [XComs](https://airflow.apache.org/concepts.html#xcoms) and pycmr should let
#   you do result.download(). Note however, that my tests show this function is
#   currently not working.
#
# d/l url in CMR response looks something like this:
# ```
# links
#   0
#       rel	"http://esipfed.org/ns/fedsearch/1.1/data#"
#       type	"application/x-hdfeos"
#       hreflang	"en-US"
#       href	"ftp://nrt3.modaps.eosdis.nasa.gov/allData/61/MYD01/2017/338/MYD01.A2017338.1915.061.NRT.hdf"
# ```
def get_esdis_path(exec_date):
    return exec_date.strftime(
     "ftp://nrt3.modaps.eosdis.nasa.gov/allData/61/MYD01/%Y/%j/MYD01.A%Y%j.%H%M.061.NRT.hdf"
     )

# TODO: replace this with PythonOperator that does
# kwargs['ti'].xcom_pull(key='download_url', task_ids='metadata_check')
# to get the the download url from upstream operator
download_granule = BashOperator(
    task_id='download_granule',
    # trigger_rule='one_success',
    bash_command="""
        wget
        --user {username}
        --password={password}
        -O {{ params.filepather.myd01(execution_date) }}
        {{ params.esdis_pather(execution_date) }}
    """,
    params={
        "filepather": satfilename,
        "esdis_pather": get_esdis_path,
        "username": secrets.ESDIS_USER,
        "password": secrets.ESDIS_PASS
    },
    dag=this_dag
)
metadata_check >> download_granule
# =============================================================================
# TODO: continue processing here...
