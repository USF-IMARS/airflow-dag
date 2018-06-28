from datetime import timedelta

from imars_dags.util.globals import QUEUE
from imars_dags.util.globals import POOL
from imars_dags.util.globals import PRIORITY


class DEFAULT_TEMPLATES:
    """
    Default argument dict templates to pass into get_default_args.

    Default arguments as a starting point for airflow dags. Extend this by
    doing something like:
    ```
    default_args = DEFAULT_ARGS.copy()
    default_args.update({
        'start_date': datetime(2017, 11, 6, 20, 25),
    })
    ```
    """
    DEFAULT = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email': [''],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        # retry_delay +1m offset to stagger scheduling
        'retry_delay': timedelta(hours=24, minutes=1),
        'queue': QUEUE.DEFAULT,
        'pool': POOL.DEFAULT,
        'priority_weight': PRIORITY.DEFAULT,
    }


def get_default_args(default_template=DEFAULT_TEMPLATES.DEFAULT, **kwargs):
    """
    Returns default_args for DAG constructor using DEFAULT_ARGS as fallback
    but allowing for overrides passed in as kwargs.
    """
    def_args = default_template.copy()
    def_args.update(kwargs)
    return def_args
