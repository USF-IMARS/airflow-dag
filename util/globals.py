"""
these are constants that can be referenced from multiple DAGs.
"""

from datetime import datetime, timedelta

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
       * limit # of jobs within in a queue to make sure systems do not get overloaded.
       * limit # of jobs working with a certain file system so we don't waste a lot of
            processing time waiting on file i/o because of too many competing workers.

    The list of pools is managed in the UI (Menu -> Admin -> Pools) by giving
        the pools a name and assigning it a number of worker slots.
    """
    SAT_SCRIPTS="sat_scripts"


""" default arguments to pass into airflow DAGs for use by Operators"""
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 7),
    'email': ['imarsroot@marine.usf.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=90),
    'queue': QUEUE.DEFAULT,
    # 'pool': 'backfill', 
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
