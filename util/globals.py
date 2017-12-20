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


class POOL:
    """
    Similar to QUEUE class.

    Pools are used to limit # of jobs of a certain type running.
    Example usages:
       * limit # of jobs working with a certain file system so we don't waste a lot of
            processing time waiting on file i/o because of too many competing workers.

    The list of pools is managed in the webserver UI (Menu -> Admin -> Pools).
    Pools with the names below must be created there before using else your
    scheduler will get confused.
    """
    DEFAULT = None    # default pool selected by not providing a value
    SLEEP   = "sleep" # pool for tasks that are just waiting / sleeping / delay
    # ^ this is used to reduce # of concurrency slots wasted just waiting.

class PRIORITY:
    """
    priority_weight is used to prevent deadlock states like when daily
    tasks can't run because a bunch of ExternalTaskSensors in the
    monthly tasks dag are waiting for the daily tasks to finish.
    """
    DEFAULT = 1
    SLEEP   = -10  # for delay, wait, etc


"""
default arguments as a starting point for airflow dags. Extend this by doing
something like:
```
default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2017, 11, 6, 20, 25),
})
```
"""
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['imarsroot@marine.usf.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(hours=24, minutes=1),  # +1m offset to stagger scheduling
    'queue': QUEUE.DEFAULT,
    'pool': POOL.DEFAULT,
    'priority_weight': PRIORITY.DEFAULT,
}


""" path to cmr.cfg file for accessing common metadata repository """
CMR_CFG_PATH="/root/airflow/dags/imars_dags/settings/cmr.cfg"
