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
    """
    DEFAULT = 'default'  # default queue any worker can pick up tasks from
    SAT_SCRIPTS = 'sat_scripts'  # only workers with sat-scripts installed &
    # functioning can pick up tasks from SAT_SCRIPTS


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
    DEFAULT = None  # default pool selected by not providing a value



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
    # 'priority_weight': 10,
}


""" path to cmr.cfg file for accessing common metadata repository """
CMR_CFG_PATH="/root/airflow/dags/imars_dags/settings/cmr.cfg"
