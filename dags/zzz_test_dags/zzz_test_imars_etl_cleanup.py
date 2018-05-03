"""

"""
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.globals import DEFAULT_ARGS
import imars_dags.dags.builders.imars_etl as imars_etl_builder

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime(2018, 3, 1, 20, 0),
    'retries': 0,
})
DAG_ID="zzz_test_imars_etl_cleanup"
TMP_DIR = imars_etl_builder.get_tmp_dir(DAG_ID)

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,  # NOTE: this & max_active_runs prevents duplicate ingests
    max_active_runs=1
) as dag:

    proc_step_one = DummyOperator(
        task_id='proc_step_one',
    )

    proc_step_two = BashOperator(
        task_id='proc_step_two',
        bash_command='/bin/false',
    )
    proc_step_one >> proc_step_two

    proc_step_three = DummyOperator(
        task_id='proc_step_three',
    )
    proc_step_two >> proc_step_three

    imars_etl_builder.add_tasks(
        dag, "", [proc_step_one], [proc_step_three], [], TMP_DIR, test=True
    )
