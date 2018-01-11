"""
Airflow processing pipeline definition for MODIS aqua per-pass processing.
Checks the coverage of each granule and triggers pass-level processing for each
region.
"""
# std libs
from datetime import timedelta, datetime
import subprocess
import configparser

# deps
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import TimeDeltaSensor
from pyCMR.pyCMR import CMR

# this package
from imars_dags.operators.MMTTriggerDagRunOperator import MMTTriggerDagRunOperator
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, CMR_CFG_PATH, SLEEP_ARGS
from imars_dags.util import satfilename
from imars_dags.settings import secrets  # NOTE: this file not in public repo!


schedule_interval=timedelta(minutes=5)

def add_tasks(dag, region, process_pass_dag_name):
    with dag as dag:
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
            delta=timedelta(hours=3),
            task_id='wait_for_data_delay',
            **SLEEP_ARGS
        )
        # =============================================================================
        # =========================================================================
        # === Trigger pass-level proc dag for the RoI if covered by the granule.
        # === Checks if this granule covers our RoI using metadata from CMR.
        # =========================================================================
        trigger_granule_dag_id = 'trigger_' + process_pass_dag_name
        coverage_check = BranchPythonOperator(
            task_id='coverage_check',
            python_callable=_coverage_check,
            provide_context=True,
            retries=10,
            retry_delay=timedelta(hours=3),
            queue=QUEUE.PYCMR,
            op_kwargs={
                'roi':region,
                'success_branch_id': trigger_granule_dag_id
            }
        )
        wait_for_data_delay >> coverage_check

        # we must trigger these manually because pass-level processing is not run
        # on a consistent schedule; it is only triggered when we have a granule
        # that covers one of our RoIs.

        trigger_pass_processing_REGION = MMTTriggerDagRunOperator(
            trigger_dag_id=process_pass_dag_name,
            python_callable=lambda context, dag_run_obj: dag_run_obj,
            execution_date="{{execution_date}}",
            task_id=trigger_granule_dag_id,
            params={
                'roi':region
            },
            retries=10,
            retry_delay=timedelta(hours=3),
            queue=QUEUE.PYCMR
        )
        coverage_check >> trigger_pass_processing_REGION
        # =========================================================================

        skip_granule = DummyOperator(
            task_id='skip_granule',
            trigger_rule='one_success'
        )
        coverage_check >> skip_granule

def get_downloadable_granule_in_roi(exec_datetime, roi):
    """
    returns pyCMR.Result if granule for given datetime is in one of our ROIs
    and is downloadable, else returns None

    NOTE: we get the granule metadata *without* server-side ROI check first
    & then do ROI check so we can be sure that the data
    has published. We want this to fail if we can't find the metadata, else
    we could end up thinking granules are not in our ROI when actually they may
    just be late to publish.
    """
    # === set up basic query for CMR
    # this basic query should ALWAYS return at least 1 result
    TIME_FMT = "%Y-%m-%dT%H:%M:%SZ"  # iso 8601
    cmr = CMR(CMR_CFG_PATH)
    time_range = str(
        (exec_datetime + timedelta(           seconds=1 )).strftime(TIME_FMT) + ',' +
        (exec_datetime + timedelta(minutes=4, seconds=59)).strftime(TIME_FMT)
    )
    search_kwargs={
        'limit':10,
        'short_name':"MYD01",  # [M]odis (Y)aqua (D) (0) level [1]
        'temporal':time_range,
        'sort_key': "-revision_date"  # this puts most recently updated first
    }
    print(search_kwargs)
    # === initial metadata check
    results = cmr.searchGranule(**search_kwargs)
    print("results:")
    print(results)
    assert(len(results) > 0)

    # === check if bounding box in res intersects with any of our ROIs and
    # === that the granule is downloadable
    # re-use the original search_kwargs, but add bounding box
    search_kwargs['bounding_box']="{},{},{},{}".format(
        roi.lonmin,  # low l long
        roi.latmin,  # low l lat
        roi.lonmax,  # up r long
        roi.latmax   # up r lat
    )
    # also add the downloadable=true criteria
    search_kwargs['downloadable']='true'
    bounded_results = cmr.searchGranule(**search_kwargs)
    if (len(bounded_results) > 0):  # granule intersects our ROI
        return bounded_results[0]  # use first granule (should be most recently updated)
    elif (len(bounded_results) == 0):  # granule does not intersect our ROI
        return None
    else:
        raise ValueError("unexpected # of results from ROI CMR search:"+str(len(bounded_results)))

def _coverage_check(ds, **kwargs):
    """
    Performs metadata check using pyCMR to decide which path the DAG should
    take. If the metadata shows the granule is in our ROI then the download
    url is written to a metadata ini file and the processing branch is followed,
    else the skip branch is followed.

    Parameters
    -----------
    ds : datetime?
        *I think* this is the execution_date for the operator instance
    kwargs['execution_date'] : datetime.datetime
        the execution_date for the operator instance (same as `ds`?)

    Returns
    -----------
    str
        name of the next operator that should trigger (ie the first in the
        branch that we want to follow)
    """
    check_region = kwargs['roi']
    exec_date = kwargs['execution_date']
    granule_result = get_downloadable_granule_in_roi(exec_date, check_region)
    if granule_result is None:
        return 'skip_granule'  # skip granule
    else:
        # === update (or create) the metadata ini file
        cfg_path = satfilename.metadata(exec_date, check_region.place_name)
        cfg = configparser.ConfigParser()
        cfg.read(cfg_path)  # returns empty config if no file
        if 'myd01' not in cfg.sections():  # + section if not exists
            cfg['myd01'] = {}
        cfg['myd01']['upstream_download_link'] = granule_result.getDownloadUrl()
        with open(cfg_path, 'w') as meta_file:
            cfg.write(meta_file)

        # === execute the processing dag for this granule & ROI
        return kwargs['success_branch_id']
