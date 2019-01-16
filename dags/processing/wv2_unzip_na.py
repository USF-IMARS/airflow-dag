# =========================================================================
# wv2 unzip to final destination
# =========================================================================
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.get_default_args import get_default_args


REGIONS = [
    ("monroe", 9),
]


for AREA_SHORT_NAME, AREA_ID in REGIONS:
    DAG_ID = get_dag_id(
        __file__, region=AREA_SHORT_NAME, dag_name="wv2_unzip"
    )
    this_dag = DAG(
        dag_id=DAG_ID,
        default_args=get_default_args(
            start_date=datetime(1980, 1, 1),
            retry_delay=timedelta(minutes=3)
        ),
        schedule_interval=None,
    )
    COMMON_LOAD_ARGS = {  # args common to all outputs
        # example file: "./058655513010_01_003/058655513010_01/
        #               058655513010_01_P001_MUL/
        #               18JUL16161240-M1BS-058655513010_01_P001.ATT"
        # example xml : "./058655513010_01_003/058655513010_01/
        #               058655513010_01_P001_MUL/
        #               18JUL16161240-M1BS-058655513010_01_P001.XML"
    }
    unzip_wv2_ingest = BashOperator(
        task_id="unzip_wv2_ingest",
        dag=this_dag,
        bash_command="wv2_unzip_na.sh",
        params=dict(
            area_id=AREA_ID
        )
    )

    # must add the dag to globals with unique name so airflow can find it
    globals()[DAG_ID] = this_dag
