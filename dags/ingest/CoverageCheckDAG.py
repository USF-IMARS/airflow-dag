from airflow.operators.sensors import TimeDeltaSensor
from airflow import DAG

from imars_dags.util.globals import SLEEP_ARGS


class CoverageCheckDAG(DAG):
    def __init__(
        self,
        check_delay,
        **kwargs
    ):
        super(CoverageCheckDAG, self).__init__(
            **kwargs
        )
        wait_for_data_delay = WaitForDataPublishSensor(
            dag=self, delta=check_delay
        )


class WaitForDataPublishSensor(TimeDeltaSensor):
    """
    =============================================================================
    === delay to wait for upstream data to become available.
    =============================================================================
    ie wait for download from OB.DAAC to complete.
    this makes sure that we don't try to run this DAG until `delta` amount of
    time past the `execution_date`
    (which is the datetime of the satellite recording).

    `delta` is the amount of time we expect between satellite measurement and
    the metadata being available in the CMR. Usually something like 2-48 hours.
    =============================================================================
    """

    def __init__(
        self,
        execution_timeout=SLEEP_ARGS['execution_timeout'],
        priority_weight=SLEEP_ARGS['priority_weight'],
        retries=SLEEP_ARGS['retries'],
        retry_delay=SLEEP_ARGS['retries'],
        task_id='wait_for_data_delay',
        **kwargs
    ):
        super(WaitForDataPublishSensor, self).__init__(
            priority_weight=priority_weight,
            retries=retries,
            retry_delay=retry_delay,
            task_id=task_id,
            **kwargs
        )
