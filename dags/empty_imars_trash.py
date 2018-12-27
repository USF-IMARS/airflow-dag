"""
archives /imars-objects/trash/* and deletes.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.get_default_args import get_default_args
from imars_dags.util.globals import QUEUE


this_dag = DAG(
    dag_id="empty_imars_trash",
    default_args=get_default_args(
        start_date=datetime(2018, 11, 20)
    ),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
)

BashOperator(
    task_id=(
        "trash_into_gdrive"
    ),
    bash_command="""
        find /srv/imars-objects/trash -type f -print0 \
        | xargs -0 -n1 -I{} \
            /opt/rclone/rclone \
                --config /srv/imars-objects/creds/rclone_airflow.conf \
                --retries 1 \
                move {} gdrive-ty:/IMARS/backups/trash/
    """,
    dag=this_dag,
    queue=QUEUE.PYCMR
)
