# deps
from airflow.operators.sensors import TimeDeltaSensor

# this package
from imars_dags.util.globals import SLEEP_ARGS


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
    DEFAULT_TASK_ID = 'wait_for_data_delay'

    def __init__(
        self,
        execution_timeout=SLEEP_ARGS['execution_timeout'],
        priority_weight=SLEEP_ARGS['priority_weight'],
        retries=SLEEP_ARGS['retries'],
        retry_delay=SLEEP_ARGS['retries'],
        task_id=DEFAULT_TASK_ID,
        **kwargs
    ):
        super(WaitForDataPublishSensor, self).__init__(
            priority_weight=priority_weight,
            retries=retries,
            retry_delay=retry_delay,
            task_id=task_id,
            **kwargs
        )
