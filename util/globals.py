"""
these are constants that can be referenced from multiple DAGs
"""

from datetime import datetime, timedelta

class QUEUE:
    """
    basically an enum class, the string values *must* match those used in
    the puppet configuration exactly so that workers can attach to the queues.
    """
    DEFAULT = 'default'  # default queue any worker can pick up tasks from
    SAT_SCRIPTS = 'sat_scripts'  # only workers with sat-scripts installed &
    # functioning can pick up tasks from SAT_SCRIPTS


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
    'queue': QUEUE.DEFAULT,  # use queues to limit job allocation to certain workers
    # 'pool': 'backfill',  # use pools to limit # of processes hitting at once
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
