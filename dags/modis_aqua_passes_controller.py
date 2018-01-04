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
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, CMR_CFG_PATH, SLEEP_ARGS
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
    delta=timedelta(hours=3),
    task_id='wait_for_data_delay',
    dag=this_dag,
    **SLEEP_ARGS
)
# =============================================================================
for region in REGIONS:
    # =============================================================================
    # === check if this granule covers our ROIs using metadata from CMR
    # =============================================================================

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
        check_region = kwargs['region']
        exec_date = kwargs['execution_date']
        granule_result = get_downloadable_granule_in_roi(exec_date, check_region)
        if granule_result is None:
            # follow the skip branch
            return "skip_granule_" + check_region.place_name
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
            return "download_granule_" + check_region.place_name

    coverage_check_REGION = BranchPythonOperator(
        task_id='coverage_check_' + region.place_name,
        python_callable=_coverage_check,
        provide_context=True,
        op_kwargs={'roi':region}
        retries=10,
        retry_delay=timedelta(hours=3),
        queue=QUEUE.PYCMR,
        dag=this_dag
    )

    wait_for_data_delay >> coverage_check_REGION
    # =============================================================================
    # === download the granule
    # =============================================================================
    # reads the download url from a metadata file created in the last step and
    # downloads the file.
    download_granule_REGION = BashOperator(
        task_id='download_granule_'+region.place_name,
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
    coverage_check_REGION >> download_granule_REGION
    # =============================================================================
    # =============================================================================
    # === do nothing on this granule, just end the DAG
    # =============================================================================
    skip_granule_REGION = DummyOperator(
        task_id='skip_granule_'+region.place_name,
        trigger_rule='one_success',
        dag=this_dag
    )
    coverage_check_REGION >> skip_granule_REGION

    # =============================================================================
    # =============================================================================
    # === trigger a processing dag for the covered regions
    # =============================================================================
    modis_aqua_process_pass_REGION = get_modis_aqua_process_pass_dag(region)

    process_pass_REGION = TriggerDagRunOperator(
        task_id='process_pass_'+region.place_name,
        trigger_dag_id="example_trigger_target_dag",
        python_callable=conditionally_trigger,
        params={'condition_param': True,
                'message': 'Hello World'},
    dag=dag)
    download_granule_REGION >> process_pass_REGION
    # =============================================================================
