"""
airflow processing pipeline definition for MODIS aqua per-pass processing
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
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, POOL, CMR_CFG_PATH
from imars_dags.util import satfilename
from imars_dags.settings.regions import REGIONS
from imars_dags.settings import secrets  # NOTE: this file not in public repo!


# one DAG for each pass
default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2017, 12, 16, 19, 30),
    'retries': 1
})
this_dag = DAG(
    'modis_aqua_passes',
    default_args=default_args,
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

def get_downloadable_granule_in_roi(exec_datetime):
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
    roi = REGIONS[0]  # we only have one region right now
    # re-use the original search_kwargs, but add bounding box
    search_kwargs['bounding_box']="{},{},{},{}".format(
        roi['lonmin'],  # low l long
        roi['latmin'],  # low l lat
        roi['lonmax'],  # up r long
        roi['latmax']   # up r lat
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
    exec_date = kwargs['execution_date']
    granule_result = get_downloadable_granule_in_roi(exec_date)
    if granule_result is None:
        # follow the skip branch
        return "skip_granule"
    else:
        # update (or create) the metadata ini file
        cfg_path = satfilename.metadata(exec_date)
        cfg = configparser.ConfigParser()
        cfg.read(cfg_path)  # returns empty config if no file
        if 'myd01' not in cfg.sections():  # + section if not exists
            cfg['myd01'] = {}
        cfg['myd01']['upstream_download_link'] = granule_result.getDownloadUrl()
        with open(cfg_path, 'w') as meta_file:
            cfg.write(meta_file)
        # follow the process branch
        return "download_granule"

coverage_check = BranchPythonOperator(
    task_id='coverage_check',
    python_callable=_coverage_check,
    provide_context=True,
    retries=5,
    retry_delay=timedelta(hours=3),
    retry_exponential_backoff=True,
    queue=QUEUE.PYCMR,
    dag=this_dag
)

wait_for_data_delay >> coverage_check
# =============================================================================
# === download the granule
# =============================================================================
# reads the download url from a metadata file created in the last step and
# downloads the file.
download_granule = BashOperator(
    task_id='download_granule',
    # trigger_rule='one_success',
    bash_command="""
        METADATA_FILE={{ params.filepather.metadata(execution_date) }} &&
        OUT_PATH={{ params.filepather.myd01(execution_date) }}         &&
        FILE_URL=$(grep "^upstream_download_link" $METADATA_FILE | cut -d'=' -f2-) &&
        wget --user={{params.username}} --password={{params.password}} --tries=1 --no-verbose --output-document=$OUT_PATH $FILE_URL
    """,
    params={
        "filepather": satfilename,
        "username": secrets.ESDIS_USER,
        "password": secrets.ESDIS_PASS
    },
    dag=this_dag
)
coverage_check >> download_granule
# =============================================================================
# =============================================================================
# === do nothing on this granule, just end the DAG
# =============================================================================
skip_granule = DummyOperator(
    task_id='skip_granule',
    trigger_rule='one_success',
    dag=this_dag
)
coverage_check >> skip_granule

# =============================================================================
# =============================================================================
# === modis GEO
# =============================================================================
l1a_2_geo = BashOperator(
    task_id='l1a_2_geo',
    bash_command="""
        export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
        /opt/ocssw/run/scripts/modis_GEO.py \
        --output={{params.geo_pather(execution_date)}} \
        {{params.l1a_pather(execution_date)}}
    """,
    params={
        'l1a_pather': satfilename.myd01,
        'geo_pather': satfilename.l1a_geo
    },
    queue=QUEUE.SAT_SCRIPTS,
    dag=this_dag
)
download_granule >> l1a_2_geo
# =============================================================================

# TODO: insert day/night check branch operator here? else ocssw will run on night granules too

# =============================================================================
# === modis l1a + geo -> l1b
# =============================================================================
make_l1b = BashOperator(
    task_id='make_l1b',
    bash_command="""
        export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
        $OCSSWROOT/run/scripts/modis_L1B.py \
        --okm={{params.okm_pather(execution_date)}} \
        --hkm={{params.hkm_pather(execution_date)}} \
        --qkm={{params.qkm_pather(execution_date)}} \
        {{params.l1a_pather(execution_date)}} \
        {{params.geo_pather(execution_date)}}
    """,
    params={
        'l1a_pather': satfilename.myd01,
        'geo_pather': satfilename.l1a_geo,
        'okm_pather': satfilename.okm,
        'hkm_pather': satfilename.hkm,
        'qkm_pather': satfilename.qkm
    },
    queue=QUEUE.SAT_SCRIPTS,
    dag=this_dag
)
l1a_2_geo >> make_l1b
download_granule >> make_l1b
# =============================================================================
# =============================================================================
# === l2gen l1b -> l2
# =============================================================================
l2gen = BashOperator(
    task_id="l2gen",
    bash_command="""
        export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
        $OCSSWROOT/run/bin/linux_64/l2gen \
        ifile={{params.l1b_pather(execution_date)}} \
        ofile={{params.l2_pather(execution_date)}} \
        geofile={{params.geo_pather(execution_date)}} \
        par={{params.parfile}}
    """,
    params={
        'l1b_pather': satfilename.okm,
        'geo_pather': satfilename.l1a_geo,
        'l2_pather':  satfilename.l2,
        'parfile': "/root/airflow/dags/imars_dags/settings/generic_l2gen.par"
    },
    queue=QUEUE.SAT_SCRIPTS,
    dag=this_dag
)
make_l1b >> l2gen
l1a_2_geo >> l2gen
# =============================================================================
