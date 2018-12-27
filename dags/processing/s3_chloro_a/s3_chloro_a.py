"""
processing for one Sentinel pass
"""
# std libs
from datetime import datetime
import os

# deps
from airflow import DAG

# this package
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.get_default_args import get_default_args
from imars_dags.operators.IMaRSETLBashOperator import IMaRSETLBashOperator
from imars_dags.util.globals import QUEUE

# | 36 | s3a_ol_1_efr
L1_PRODUCT_ID = 36
L2_PRODUCT_ID = 50
L3_PRODUCT_ID = 51

FLY_AREA_ID = 52
FLY_AREA_SHORT_NAME = "fl_bay"
OKA_AREA_ID = 53
OKA_AREA_SHORT_NAME = "okeecho"
PIN_AREA_ID = 54
PIN_AREA_SHORT_NAME = "pinellas"
CHAR_AREA_ID = 55
CHAR_AREA_SHORT_NAME = "char_bay"

AREA_SHORT_NAME = "_florida"
AREA_ID = 49

DAG_ID = get_dag_id(
    __file__, region=AREA_SHORT_NAME, dag_name="s3_chloro_a"
)
THIS_DIR = os.path.dirname(os.path.realpath(__file__))

this_dag = DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(
        start_date=datetime.utcnow()
    ),
    schedule_interval=None,
)

l1_to_l2 = IMaRSETLBashOperator(
    task_id='l1_to_l2',
    bash_command="l1_to_l2.sh",
    inputs={
        "s3_file":
            "product_id="+str(L1_PRODUCT_ID)+" AND date_time='{{ts}}'"
    },
    outputs={
        'l2_file': {
            "sql": (
                "product_id={} AND area_id={} ".format(
                        L2_PRODUCT_ID, AREA_ID
                ) +
                " AND date_time='{{ execution_date }}'"  # TODO: rm?
            ),
            "json": '{'  # noqa E131
                '"area_short_name":"' + AREA_SHORT_NAME + '"'
            '}',
        },
    },
    params={
        "par": os.path.join(
            THIS_DIR,  # here
            "IMaRS_S3_l2gen.par"
        ),
    },
    queue=QUEUE.SAT_SCRIPTS,
    dag=this_dag,
)

l2_to_l3 = IMaRSETLBashOperator(
    task_id='l2_to_l3',
    bash_command="process_S3_3.sh",
    inputs={
        "s2_file":
            "product_id="+str(L2_PRODUCT_ID)+" AND date_time='{{ts}}'"
    },
    outputs={
        'c_map': {
            "product_id": L3_PRODUCT_ID,
            "time": "{{ ts }}",
            "sql": (
                "product_id={} AND area_id={} ".format(
                        L3_PRODUCT_ID, CHAR_AREA_ID
                ) +
                " AND date_time='{{ execution_date }}'"
            ),
            "json": '{'  # noqa E131
                '"area_short_name":"' + CHAR_AREA_SHORT_NAME + '"'
            '}',
        },
        'p_map': {
            "product_id": L3_PRODUCT_ID,
            "time": "{{ ts }}",
            "sql": (
                "product_id={} AND area_id={} ".format(
                        L3_PRODUCT_ID, PIN_AREA_ID
                ) +
                " AND date_time='{{ execution_date }}'"  # TODO: rm?
            ),
            "json": '{'  # noqa E131
                '"area_short_name":"' + PIN_AREA_SHORT_NAME + '"'
            '}',
        },
        'o_map': {
            "product_id": L3_PRODUCT_ID,
            "time": "{{ ts }}",
            "sql": (
                "product_id={} AND area_id={} ".format(
                        L3_PRODUCT_ID, OKA_AREA_ID
                ) +
                " AND date_time='{{ execution_date }}'"  # TODO: rm?
            ),
            "json": '{'  # noqa E131
                '"area_short_name":"' + OKA_AREA_SHORT_NAME + '"'
            '}',
        },
        'f_map': {
            "product_id": L3_PRODUCT_ID,
            "time": "{{ ts }}",
            "sql": (
                "product_id={} AND area_id={} ".format(
                        L3_PRODUCT_ID, FLY_AREA_ID
                ) +
                " AND date_time='{{ execution_date }}'"  # TODO: rm?
            ),
            "json": '{'  # noqa E131
                '"area_short_name":"' + FLY_AREA_SHORT_NAME + '"'
            '}',
        },
    },
    params={
        "xml_filec": os.path.join(
            THIS_DIR,
            "map_CHAR_S3_OLCI.xml"
        ),
         "xml_filep": os.path.join(
            THIS_DIR,
            "map_PIN_S3_OLCI.xml"
        ),
        "xml_fileo": os.path.join(
            THIS_DIR,
            "map_OKA_S3_OLCI.xml"
        ),
        "xml_filef": os.path.join(
            THIS_DIR,
            "map_FLBY_S3_OLCI.xml"
        ),
    },
    queue=QUEUE.SAT_SCRIPTS,
    dag=this_dag,
)

l1_to_l2 >> l2_to_l3
