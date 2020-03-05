"""
processing for one Sentinel pass
"""
# std libs
from datetime import datetime
import os

# deps
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# this package
from imars_dags.util.globals import QUEUE
from imars_dags.util.Area import Area

DAG_NAME = os.path.splitext(os.path.basename(__file__))[0]

AREAS = [
    "florida"
]
# | 36 | s3a_ol_1_efr
# | 49 | s3a_ol_1_efr_l2          |
# | 50 | s3a_ol_1_efr_l3          |
L1_PRODUCT_ID = 36
L2_PRODUCT_ID = 49
L3_PRODUCT_ID = 50

for area_short_name in AREAS:
    area = Area(area_short_name)
    AREA_SHORT_NAME = area.short_name
    AREA_ID = area.id

    DAG_ID = "s3_chloro_a_{}".format(area_short_name)
    THIS_DIR = os.path.dirname(os.path.realpath(__file__))

    this_dag = DAG(
        dag_id=DAG_ID,
        default_args={
            "start_date": datetime(2016, 2, 16)  # S3-A launch
        },
        schedule_interval=None,
    )
    this_dag.doc_md = __doc__

    l1_to_l2 = BashOperator(
        task_id='l1_to_l2',
        bash_command="l1_to_l2.sh",
        params={
            "par": os.path.join(
                THIS_DIR,  # here
                "IMaRS_S3_l2gen.par"
            ),
            "l1_pid": L1_PRODUCT_ID,
            "l2_pid": L2_PRODUCT_ID,
            "area_id": AREA_ID,
            "area_short_name": AREA_SHORT_NAME,
        },
        queue=QUEUE.OCSSW,
        dag=this_dag,
    )

    l2_to_l3 = BashOperator(
        task_id='l2_to_l3',
        bash_command="l2_to_l3.sh",
        params={
            "p_id": L3_PRODUCT_ID,
            "input_pid": L2_PRODUCT_ID,
            "input_area_id": AREA_ID,
            "area_id": AREA_ID,
            "gpt_xml": os.path.join(
                THIS_DIR,
                "map_FL_S3_OLCI.xml"  # TODO: create this file
            ),
        },
        queue=QUEUE.SNAP,
        dag=this_dag,
    )

    l1_to_l2 >> l2_to_l3

    # must add the dag to globals with unique name so
    # airflow can find it
    globals()[this_dag.dag_id] = this_dag

# === DEPRECATED FL SUB-REGIONS:
# TODO: Remove these or do we still want them though the FL prod fully covers?
#
# FLY_AREA_ID = 13
# # FLY_AREA_SHORT_NAME = "fl_bay"
# l3_fl_bay = BashOperator(
#     task_id='l3_fl_bay',
#     bash_command="l2_to_l3.sh",
#     params={
#         "input_area_id": AREA_ID,
#         "input_pid": L2_PRODUCT_ID,
#         "p_id": L3_PRODUCT_ID,
#         "area_id": FLY_AREA_ID,
#         "gpt_xml": os.path.join(
#             THIS_DIR,
#             "map_FLBY_S3_OLCI.xml"
#         ),
#     },
#     queue=QUEUE.SAT_SCRIPTS,
#     dag=this_dag,
# )
#
# OKA_AREA_ID = 14
# # OKA_AREA_SHORT_NAME = "okeecho"
# l3_okeecho = BashOperator(
#     task_id='l3_okeecho',
#     bash_command="l2_to_l3.sh",
#     params={
#         "input_area_id": AREA_ID,
#         "input_pid": L2_PRODUCT_ID,
#         "p_id": L3_PRODUCT_ID,
#         "area_id": OKA_AREA_ID,
#         "gpt_xml": os.path.join(
#             THIS_DIR,
#             "map_OKA_S3_OLCI.xml"
#         ),
#     },
#     queue=QUEUE.SAT_SCRIPTS,
#     dag=this_dag,
# )
#
# PIN_AREA_ID = 15
# # PIN_AREA_SHORT_NAME = "pinellas"
# l3_pinellas = BashOperator(
#     task_id='l3_pinellas',
#     bash_command="l2_to_l3.sh",
#     params={
#         "input_area_id": AREA_ID,
#         "input_pid": L2_PRODUCT_ID,
#         "p_id": L3_PRODUCT_ID,
#         "area_id": PIN_AREA_ID,
#         "gpt_xml": os.path.join(
#             THIS_DIR,
#             "map_PIN_S3_OLCI.xml"
#         ),
#     },
#     queue=QUEUE.SAT_SCRIPTS,
#     dag=this_dag,
# )
#
# CHAR_AREA_ID = 16
# # CHAR_AREA_SHORT_NAME = "char_bay"
# l3_char_bay = BashOperator(
#     task_id='l3_char_bay',
#     bash_command="l2_to_l3.sh",
#     params={
#         "input_area_id": AREA_ID,
#         "input_pid": L2_PRODUCT_ID,
#         "p_id": L3_PRODUCT_ID,
#         "area_id": CHAR_AREA_ID,
#         "gpt_xml": os.path.join(
#             THIS_DIR,
#             "map_CHAR_S3_OLCI.xml"
#         ),
#     },
#     queue=QUEUE.SAT_SCRIPTS,
#     dag=this_dag,
# )
#
# l1_to_l2 >> l3_fl_bay
# l1_to_l2 >> l3_okeecho
# l1_to_l2 >> l3_pinellas
# l1_to_l2 >> l3_char_bay
