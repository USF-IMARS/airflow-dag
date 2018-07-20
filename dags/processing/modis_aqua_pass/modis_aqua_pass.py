"""
processing for one modis pass
"""
# std libs
from datetime import datetime
import os

# deps
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG

# this package
from imars_dags.util.get_default_args import get_default_args
from imars_dags.operators.IMaRSETLBashOperator import IMaRSETLBashOperator
from imars_dags.util.globals import QUEUE
from imars_dags.util._render import _render
from imars_dags.util.get_dag_id import get_dag_id

AREA_SHORT_NAME = "gom"

this_dag = DAG(
    dag_id=get_dag_id(
        __file__, region=AREA_SHORT_NAME, dag_name="modis_aqua_pass"
    ),
    default_args=get_default_args(
        start_date=datetime.utcnow()
    ),
    user_defined_filters=dict(
        render=_render
    ),
    schedule_interval=None,
)


l1_to_l2 = IMaRSETLBashOperator(
    task_id='l1_to_l2',
    bash_command="l1_to_l2.sh",
    inputs={
        "myd01_file": "product_id=5 AND date_time='{{ts}}'"
    },
    outputs={
        'l2_file': {
            "verbose": 3,
            "product_id": 35,
            # "time": "{{ ts }}",  # .replace(" ", "T") ?
            # "datetime": {{ execution_date }},
            "json": '{'
                '"status_id":3,'  # noqa E131
                '"area_id":1,'
                '"area_short_name":"' + AREA_SHORT_NAME + '"'
            '}'
        },
    },
    tmpdirs=["tmp_dir"],
    params={
        "par": os.path.join(
            os.path.dirname(os.path.realpath(__file__)),  # here
            "moda_l2gen.par"
        ),
    },
    queue=QUEUE.SAT_SCRIPTS,
    trigger_rule=TriggerRule.ONE_SUCCESS,  # run if any upstream passes
    dag=this_dag,
)
