from datetime import timedelta

from airflow.operators.python_operator import BranchPythonOperator

from imars_dags.dag_classes.ingest.CoverageCheckDAG import \
    ROI_COVERED_BRANCH_ID, ROI_NOT_COVERED_BRANCH_ID


class CoverageBranchOperator(BranchPythonOperator):
    """
    # =========================================================================
    # === Checks if this granule covers our RoI using metadata.
    # =========================================================================
    writes download url to ini file at metadata_filepath
    """
    def __init__(
        self,
        metadata_filepath,
        python_callable,
        roi,
        op_kwargs={},
        task_id='coverage_check',
        provide_context=True,
        retries=0,
        retry_delay=timedelta(minutes=1),
        success_branch_id=ROI_COVERED_BRANCH_ID,
        fail_branch_id=ROI_NOT_COVERED_BRANCH_ID,
        **kwargs
    ):
        # === set cmr_search_kwargs within op_kwargs
        op_kwargs['metadata_filepath'] = op_kwargs.get(
            'metadata_filepath', metadata_filepath
        )
        op_kwargs['roi'] = op_kwargs.get(
            'roi', roi
        )
        op_kwargs['fail_branch_id'] = op_kwargs.get(
            'fail_branch_id', fail_branch_id
        )
        op_kwargs['success_branch_id'] = op_kwargs.get(
            'success_branch_id', success_branch_id
        )

        super(CoverageBranchOperator, self).__init__(
            task_id=task_id,
            python_callable=python_callable,
            provide_context=provide_context,
            retries=retries,
            retry_delay=retry_delay,
            op_kwargs=op_kwargs,
            **kwargs
        )
