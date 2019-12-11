"""
Updates symlinks to point at latest data for github.com/USF-IMARS/img-dash
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from imars_dags.util.get_default_args import get_default_args
from imars_dags.dags.update_dash_symlinks._update_dash_symlinks \
    import main as update_symlinks_fn


this_dag = DAG(
    dag_id="update_dash_symlinks",
    default_args=get_default_args(
        start_date=datetime(2018, 7, 30)
    ),
    schedule_interval="0 0 * * *",  # @daily TODO: weekly instead?
    catchup=False,
    max_active_runs=1,
)
this_dag.doc_md = __doc__

update_symlinks = PythonOperator(
    task_id="update_symlinks",
    python_callable=update_symlinks_fn,
    dag=this_dag,
)
