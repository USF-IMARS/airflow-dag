"""
Reads each csv file and pushes the data into graphite.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


this_dag = DAG(
    dag_id="csvs_to_graphite",
    default_args={
            "start_date": datetime(2014, 1, 1)
    },
    schedule_interval="00 12 * * * *",
    catchup=False,
    max_active_runs=1,
)
this_dag.doc_md = __doc__

with this_dag as dag:
    # === FGBNMS
    # TODO: I think fgbnms_csvs2graphite has been replaced by fgb_sat_region.
    #       rm it?
    fgbnms_csvs2graphite = BashOperator(
        task_id=(
            "fgbnms_csvs2graphite"
        ),
        bash_command="""
            python2 \
            /home/airflow/dags/imars_dags/dags/csvs_to_graphite/py_scripts/fgbnms_csvs2graph.py
        """,
    )
    fgb_river_ts = BashOperator(
        task_id=(
            "fgb_river_ts"
        ),
        bash_command="""
            python2 \
            /home/airflow/dags/imars_dags/dags/csvs_to_graphite/py_scripts/fgb_river_ts.py
        """,
    )
    fgb_sat_region = BashOperator(
        task_id=(
            "fgb_sat_region"
        ),
        bash_command="""
            python2 \
            /home/airflow/dags/imars_dags/dags/csvs_to_graphite/py_scripts/fgb_sat_region.py
        """,
    )

    # === FKNMS
    fk_sat_region = BashOperator(
        task_id=(
            "fk_sat_region"
        ),
        bash_command="""
            python2 \
            /home/airflow/dags/imars_dags/dags/csvs_to_graphite/py_scripts/fk_sat_region.py
        """,
    )
    fk_river_ts = BashOperator(
        task_id=(
            "fk_river_ts"
        ),
        bash_command="""
            python2 \
            /home/airflow/dags/imars_dags/dags/csvs_to_graphite/py_scripts/fk_river_ts.py
        """,
    )
    fk_bouy_ts = BashOperator(
        task_id=(
            "fk_bouy_ts"
        ),
        bash_command="""
            python2 \
            /home/airflow/dags/imars_dags/dags/csvs_to_graphite/py_scripts/fk_bouy_ts.py
        """,
    )
