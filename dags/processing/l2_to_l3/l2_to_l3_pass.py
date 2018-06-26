"""
# =========================================================================
# === L3 Generation using GPT graph
# =========================================================================
# assumes the needed l2 files have already been generated
#
# example cmd:
#     /opt/snap/bin/gpt L3G_MODA_GOM_vIMARS.xml
#     -t /home1/scratch/epa/satellite/modis/GOM/L3G_OC/A2017313_map.nc
#     -f NetCDF-BEAM
#     /srv/imars-objects/modis_aqua_gom/l2/A2017313174500.L2
#     /srv/imars-objects/modis_aqua_gom/l2/A2017313192000.L2
#     /srv/imars-objects/modis_aqua_gom/l2/A2017313192500.L2
#
#     -t is the target (output) file, -f is the format
"""
# std libs
from datetime import datetime
from datetime import timedelta
import os

# deps
from airflow.operators.sensors import TimeDeltaSensor
from airflow.operators.bash_operator import BashOperator
from airflow import DAG

# this package
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.util.etl_tools.extract import add_extract
from imars_dags.util.etl_tools.load import add_load
from imars_dags.util.etl_tools.cleanup import add_cleanup
from imars_dags.util.globals import QUEUE
from imars_dags.util.globals import DEFAULT_ARGS
from imars_dags.util.globals import SLEEP_ARGS


AREA_SHORT_NAME = "gom"
L2_PRODUCT_ID = 35
L3_PRODUCT_ID = 42

THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
xml_file = os.path.join(
    THIS_FILE_DIR,
    "L3G_MODA_GOM_v2.xml"
)

DEF_ARGS = DEFAULT_ARGS.copy()
DEF_ARGS.update({
    'start_date': datetime.utcnow(),
})

this_dag = DAG(
    dag_id=get_dag_id(__file__, region=AREA_SHORT_NAME),
    default_args=DEF_ARGS,
    schedule_interval=None,
)

# ===========================================================================
# === EXTRACT INPUT FILE(S) ===
# ===========================================================================
l2_input = tmp_filepath(this_dag.dag_id, 'l2_input')
extract_l2_input = add_extract(
    this_dag,
    "product_id={}".format(L2_PRODUCT_ID),
    l2_input
)
# =========================================================================
# =========================================================================
# === l3gen gpt command
# =========================================================================
l3_output = tmp_filepath(this_dag.dag_id, 'l3_output')

l3gen = BashOperator(
    task_id="l3gen",
    bash_command="""
        /opt/snap/bin/gpt """+xml_file+""" \
        -t """+l3_output+""" \
        -f NetCDF-BEAM \
        """+l2_input+"""
    """,
    queue=QUEUE.SNAP,
    dag=this_dag,
)
# =========================================================================
# =========================================================================
# === LOAD OUTPUT FILE(S) ===
# =========================================================================
load_l3_list = add_load(
    this_dag,
    to_load=[
        {
            "filepath": l3_output,  # required!
            "verbose": 3,
            "product_id": L3_PRODUCT_ID,
            "json": '{'
                '"status_id":3,'  # noqa E131
                '"area_id":1,'
                '"area_short_name":"' + AREA_SHORT_NAME + '"'
            '}'
        }
    ],
    upstream_operators=[l3gen]
)

cleanup_task = add_cleanup(
    this_dag,
    to_cleanup=[l2_input, l3_output],
    upstream_operators=load_l3_list
)

# =========================================================================
# === connect it all up
# =========================================================================
extract_l2_input >> l3gen
# `l3gen >> load_l3_list` done by `upstream_operators=[l2gen]`
# `load_l3_list >> cleanup_task` via `upstream_operators=[load_l2_list]`
# =========================================================================
