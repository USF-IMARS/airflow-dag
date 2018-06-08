from datetime import timedelta
import os

from airflow.operators.python_operator import BranchPythonOperator
from pyCMR.pyCMR import CMR

from imars_dags.util.etl_tools.tmp_file import tmp_filepath

# path to cmr.cfg file for accessing common metadata repository
CMR_CFG_PATH=os.path.join(
    os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/ingest/cmr
    "cmr.cfg"
)

def get_downloadable_granule_in_roi(exec_datetime, roi, cmr_search_kwargs):
    """
    returns pyCMR.Result if granule for given datetime is in one of our ROIs
    and is downloadable and is during the day, else returns None

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
    cmr_search_kwargs['limit'] = 10
    cmr_search_kwargs['temporal'] = time_range
    cmr_search_kwargs['sort_key'] = "-revision_date" # most recently updated 1st

    print(cmr_search_kwargs)
    # === initial metadata check
    results = cmr.searchGranule(**cmr_search_kwargs)
    print("results:")
    print(results)
    assert(len(results) > 0)

    # === check if bounding box in res intersects with any of our ROIs and
    # === that the granule is downloadable
    # re-use the original search_kwargs, but add bounding box
    cmr_search_kwargs['bounding_box']="{},{},{},{}".format(
        roi.lonmin,  # low l long
        roi.latmin,  # low l lat
        roi.lonmax,  # up r long
        roi.latmax   # up r lat
    )
    bounded_results = cmr.searchGranule(**cmr_search_kwargs)
    if (len(bounded_results) > 0):  # granule intersects our ROI
        return bounded_results[0]  # use first granule (should be most recently updated)
    elif (len(bounded_results) == 0):  # granule does not intersect our ROI
        return None
    else:
        raise ValueError(
            "unexpected # of results from ROI CMR search:" +
            str(len(bounded_results))
        )


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
    cmr_search_kwargs = kwargs['cmr_search_kwargs']
    granule_result = get_downloadable_granule_in_roi(
        exec_date, check_region, cmr_search_kwargs
    )
    if granule_result is None:
        return kwargs['fail_branch_id']  # skip granule
    else:

        # TODO: this should write to imars_product_metadata instead?!?

        # === update (or create) the metadata ini file
        # path might have airflow macros, so we need to render
        task = kwargs['task']
        cfg_path = kwargs['metadata_filepath']
        cfg_path = task.render_template(
            '',
            cfg_path,
            kwargs
        )
        cfg = configparser.ConfigParser()
        cfg.read(cfg_path)  # returns empty config if no file
        if 'myd01' not in cfg.sections():  # + section if not exists
            cfg['myd01'] = {}
        cfg['myd01']['upstream_download_link'] = str(granule_result.getDownloadUrl())
        with open(cfg_path, 'w') as meta_file:
            cfg.write(meta_file)

        return kwargs['success_branch_id']  # download granule


class CMRCoverageBranchOperator(BranchPythonOperator):
    """
    # =========================================================================
    # === Checks if this granule covers our RoI using metadata.
    # =========================================================================
    writes download url to ini file at metadata_filepath
    """
    ROI_COVERED_BRANCH_ID='download_granule'
    ROI_NOT_COVERED_BRANCH_ID='skip_granule'
    def __init__(
        self,
        cmr_search_kwargs,
        roi,
        metadata_filepath,
        task_id='coverage_check',
        python_callable=_coverage_check,
        provide_context=True,
        retries=0,
        retry_delay=timedelta(minutes=1),
        op_kwargs={},
        success_branch_id=ROI_COVERED_BRANCH_ID,
        fail_branch_id=ROI_NOT_COVERED_BRANCH_ID,
        **kwargs
    ):
        """
        cmr_search_kwargs : dict
            search_kwargs dict to pass to pyCMR.
            Example:
                {'short_name': 'MYD01'}
        """
        # === set cmr_search_kwargs within op_kwargs
        op_kwargs['cmr_search_kwargs'] = op_kwargs.get(
            'cmr_search_kwargs', cmr_search_kwargs
        )
        op_kwargs['metadata_filepath'] = op_kwargs.get(
            'metadata_filepath', metadata_filepath
        )
        op_kwargs['roi'] = op_kwargs.get(
            'roi', roi
        )
        op_kwargs['fail_branch_id'] = op_kwargs.get(
            'fail_branch_id',fail_branch_id
        )
        op_kwargs['success_branch_id'] = op_kwargs.get(
            'success_branch_id', success_branch_id
        )

        super(CMRCoverageBranchOperator, self).__init__(
            task_id=task_id,
            python_callable=python_callable,
            provide_context=provide_context,
            retries=retries,
            retry_delay=retry_delay,
            op_kwargs=op_kwargs,
            **kwargs
        )
