"""
airflow processing pipeline definition for MODIS aqua per-pass processing
"""
# std libs
from datetime import timedelta
import subprocess

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

def get_granule_in_roi(exec_datetime):
    """
    returns pyCMR.Result if granule for given datetime is in one of our ROIs,
    else returns None

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
        'temporal':time_range,
        'sort_key': "-revision_date"  # this puts most recently updated first
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
        return bounded_results[0]  # use first granule TODO: choose more cleverly
    elif (len(bounded_results) == 0):  # granule does not intersect our ROI
        return None
    else:
        raise ValueError("unexpected # of results from ROI CMR search:"+str(len(bounded_results)))

def _metadata_check(ds, **kwargs):
    """
    Performs metadata check using pyCMR to decide which path the DAG should
    take. If the metadata shows the granule is in our ROI then the granule
    is downloaded and the processing branch is followed, else the skip branch
    is followed.

    Parameters
    -----------
    ds : datetime?
        *I think* this is the execution_date for the operator instance
    kwargs['execution_date'] : datetime.datetime
        the execution_date for the operator instance (same as `ds`?)
    kwargs['ti'] : task-instance?
        something that we use to push/pull xcoms?

    Returns
    -----------
    str
        name of the next operator that should trigger (ie the first in the
        branch that we want to follow)
    """
    exec_date = kwargs['execution_date']
    granule_result = get_granule_in_roi(exec_date)
    if granule_result is None:
        return "skip_granule"
    else:
        # granule_result.download()
        # NOTE: pyCMR downloading is borked at the moment and fails silently
        # https://github.com/7yl4r/pyCMR/commit/0ae16d40df1abd1eb6250b4326104e33937b3537
        # https://github.com/7yl4r/pyCMR/commit/645561fd4730f0f4c2572fe20f44e0a6790e7dc0
        # so let's download it manually...
        #
        # since we can't get the url using pyCMR (see
        # https://github.com/nasa/pyCMR/issues/41 and #27), let's try
        # guessing some URLs! :grimacing:
        urls_to_try=[  # in order of preference...
            "ftp://ladsweb.nascom.nasa.gov/allData/{version}/MYD01/%Y/%j/MYD01.A%Y%j.%H%M.{version_zfilled}.%Y{last_modified_time}.hdf",  # std product
            "ftp://nrt3.modaps.eosdis.nasa.gov/allData/{version}/MYD01/%Y/%j/MYD01.A%Y%j.%H%M.{version_zfilled}.NRT.hdf",  # nrt
            "ftp://nrt4.modaps.eosdis.nasa.gov/allData/{version}/MYD01/%Y/%j/MYD01.A%Y%j.%H%M.{version_zfilled}.NRT.hdf",  # nrt
            "ftp://nrt1.modaps.eosdis.nasa.gov/allData/{version}/MYD01/%Y/%j/MYD01.A%Y%j.%H%M.{version_zfilled}.NRT.hdf",  # nrt
            "ftp://nrt2.modaps.eosdis.nasa.gov/allData/{version}/MYD01/%Y/%j/MYD01.A%Y%j.%H%M.{version_zfilled}.NRT.hdf"  # nrt
        ]
        versions=['61','6']  # also in order of preference
        for vers in versions:
            for url in urls_to_try:
                source = exec_date.strftime(url.format(
                    version=vers,
                    version_zfilled=vers.zfill(3),
                    last_modified_time="[000000000,999999999]"  # this is the time the file was last modified on the ftp server
                    # we have no way of knowing this, so we have to use regex with wget's -A option
                    # https://unix.stackexchange.com/a/117998/122077
                ))
                target = satfilename.myd01(exec_date)
                try:
                    url_parts = source.split('/')
                    filename = url_parts[-1]
                    url_dir = '/'.join(url_parts[:-1])
                    returncode = subprocess.call(
                        "wget -A={fname} --user={username} --password={password} --tries=1 --output-document={tgt} {src}".format(
                            username=secrets.ESDIS_USER,
                            password=secrets.ESDIS_PASS,
                            src=url_dir,
                            tgt=target,
                            fname=filename
                        ),
                        shell=True
                    )
                    if returncode == 0:  # download was successful!
                        return "process_granule"
                except FileNotFoundError as f_err:
                    # NOTE: [docs](https://docs.python.org/3/library/subprocess.html#call-function-trio)
                    # say that subprocess.call does not throw errors... but... it does. :shrug:
                    print('subprocess call throws error:')
                    print(f_err)
        # if we didn't return yet then we couldn't download any of them
        raise FileNotFoundError("could not get granule from any urls")


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
# === process the granule
# =============================================================================
process_granule = DummyOperator(  # TODO: cp processing tasks from modis_aqua_processing
    task_id='process_granule',
    trigger_rule='one_success',
    dag=this_dag
)
metadata_check >> process_granule

# TODO: continue processing here...
