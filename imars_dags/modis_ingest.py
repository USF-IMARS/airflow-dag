"""
airflow processing pipeline definition for MODIS data
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

class QUEUE:
    """
    basically an enum class, the string values *must* match those used in
    the puppet configuration exactly so that workers can attach to the queues.
    """
    DEFAULT = 'default'  # default queue any worker can pick up tasks from
    SAT_SCRIPTS = 'sat_scripts'  # only workers with sat-scripts installed &
    # functioning can pick up tasks from SAT_SCRIPTS

default_args = {
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

dag_ingest = DAG('modis', default_args=default_args, schedule_interval=timedelta(hours=6))

# =============================================================================
# === Modis ingest subscription(s)
# =============================================================================
modis_ingest = BashOperator(
    task_id='subscription_1310',
    bash_command='/opt/RemoteDownlinks/ingest_subscription.py',
    dag=dag_ingest
)
# NOTE: this writes files out to /srv/imars-objects/subscription-1310/modis_l0
# example filenames:
# MOD00.A2017318.0430_1.PDS.bz2
# MOD00.A2017309.0115_1.PDS.bz2
# =============================================================================
