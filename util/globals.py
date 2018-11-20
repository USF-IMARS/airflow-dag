"""
these are constants that can be referenced from multiple DAGs.
"""

from datetime import timedelta


class QUEUE:
    """
    Basically an enum, the string values *must* match those used in
    the puppet configuration exactly so that workers can attach to the queues.

    Queues are used to limit job allocation to certain workers. Useful when
    a task has specific software requriments that may not be met on all users.
    For example: Only workers with sat-scripts installed & functioning can
    pick up tasks from the SAT_SCRIPTS queue.
    """
    DEFAULT = 'default'  # default queue any worker can pick up tasks from

    SAT_SCRIPTS = 'sat_scripts'  # https://github.com/USF-IMARS/sat-scripts/
    PYCMR = 'pycmr'  # https://github.com/7yl4r/pyCMR
    SNAP = 'snap'  # https://github.com/USF-IMARS/snap-puppet
    RCLONE = 'rclone'  # https://rclone.org/ configured by puppet

    MATLAB = 'matlab'
    WV2_PROC = 'wv2_proc'  # https://github.com/USF-IMARS/wv2-processing
    # WV2_PROC requires MATLAB... so WV2_PROC should imply MATLAB as well?!?


class POOL:
    """
    Similar to QUEUE class.

    Pools are used to limit # of jobs of a certain type running.
    Example usages:
       * limit # of jobs working with a certain file system so we don't waste
            processing time waiting on file i/o because of competing workers.

    The list of pools is managed in the webserver UI (Menu -> Admin -> Pools).
    Pools with the names below must be created there before using else your
    scheduler will get confused.

    NOTE: pool limits are currently "soft"er than they should be see:
        https://issues.apache.org/jira/browse/AIRFLOW-584
    """
    DEFAULT = None    # default pool selected by not providing a value
    # SLEEP = "sleep" # pool for tasks that are just waiting / sleeping / delay
    # ^ this is used to reduce # of concurrency slots wasted just waiting.


class PRIORITY:
    """
    priority_weight is used to prevent deadlock states like when daily
    tasks can't run because a bunch of ExternalTaskSensors in the
    monthly tasks dag are waiting for the daily tasks to finish.

    NOTE: uhhh... task priority seems to be ignored right now?
    """
    DEFAULT = 1
    SLEEP = -10  # for delay, wait, etc


"""
This argument dict can be used like a macro to pass in settings for tasks
that are just sleeping/waiting/delaying.

These args are set to prevent deadlocks from having a lot of "wait"
tasks. In theory "pool"s are all that is needed for this, but I have not
been able to make that work. After waiting for a time the wait task will
fail and be set for retry until pass or timeout.

Note that if you want to override one of these you must do something like:
```
cust_args = SLEEP_ARGS.copy()
cust_args.update({
    'start_date': datetime(2017, 11, 6, 20, 25),
})
```
NOT `SLEEP_ARGS['key'] = 0`, because this modifies the constant.
"""
SLEEP_ARGS = {
    'retries': 168,  # 24*7
    'retry_delay': timedelta(hours=1),
    'retry_exponential_backoff': False,
    'execution_timeout': timedelta(seconds=2),
    'priority_weight': PRIORITY.SLEEP
    # 'pool': POOL.SLEEP
}
