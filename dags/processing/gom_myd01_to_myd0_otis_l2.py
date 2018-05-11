"""
manually triggered dag that runs processing for one modis pass
"""
# std libs
from datetime import datetime, timedelta
import subprocess
import configparser
import os

# deps
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG

# this package
import imars_dags.dags.builders.imars_etl as imars_etl_builder
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS
from imars_dags.util import satfilename
from imars_dags.settings import secrets  # NOTE: this file not in public repo!
from imars_dags.regions import gom, fgbnms, ao1

DEF_ARGS = DEFAULT_ARGS.copy()
DEF_ARGS.update({
    'start_date': datetime.utcnow(),
    'retries': 1
})

SCHEDULE_INTERVAL=None
REGION=gom
PARFILE=os.path.join(
    os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/gom/
    "moda_l2gen.par"
)

this_dag = DAG(
    dag_id="gom_myd01_to_myd0_otis_l2",
    default_args=DEF_ARGS,
    schedule_interval=SCHEDULE_INTERVAL
)

TMP_DIR = imars_etl_builder.get_tmp_dir(this_dag.dag_id)
with this_dag as dag:
    # =========================================================================
    # === modis GEO
    # =========================================================================
    GEOFILE=TMP_DIR+'/geo'
    l1a_2_geo = BashOperator(
        task_id='l1a_2_geo',
        bash_command="""
            export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
            OUT_PATH="""+GEOFILE+""" && \
            /opt/ocssw/run/scripts/modis_GEO.py \
            --output=$OUT_PATH \
            {{ ti.xcom_pull(task_ids="extract_file") }} && \
            && [[ -s $OUT_PATH ]]
        """,
        queue=QUEUE.SAT_SCRIPTS,
        trigger_rule=TriggerRule.ONE_SUCCESS  # run if any upstream passes
    )
    # =========================================================================

    # TODO: insert day/night check branch operator here? else ocssw will run on night granules too

    # =========================================================================
    # === modis l1a + geo -> l1b
    # =========================================================================
    OKMFILE=TMP_DIR+'/okm'  # aka L1b
    HKMFILE=TMP_DIR+'/hkm'
    QKMFILE=TMP_DIR+'/qkm'

    make_l1b = BashOperator(
        task_id='make_l1b',
        bash_command="""
            export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
            OKM_PATH="""+OKMFILE+""" && \
            HKM_PATH="""+HKMFILE+""" && \
            QKM_PATH="""+QKMFILE+""" && \
            $OCSSWROOT/run/scripts/modis_L1B.py \
            --okm=$OKM_PATH \
            --hkm=$HKM_PATH \
            --qkm=$QKM_PATH \
            {{ ti.xcom_pull(task_ids="extract_file") }} \
            """+GEOFILE+""" \
            && [[ -s $OKM_PATH && -s $HKM_PATH && -s $QKM_PATH ]]
        """,
        queue=QUEUE.SAT_SCRIPTS
    )
    # =========================================================================
    # =========================================================================
    # === l2gen l1b -> l2
    # =========================================================================
    L2FILE=TMP_DIR+'/l2'
    l2gen = BashOperator(
        task_id="l2gen",
        bash_command="""
            export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
            $OCSSWROOT/run/bin/linux_64/l2gen \
            ifile="""+OKMFILE+""" \
            ofile="""+L2FILE+""" \
            geofile="""+GEOFILE+""" \
            par={{params.parfile}}
        """,
        params={
            'parfile': PARFILE,
        },
        queue=QUEUE.SAT_SCRIPTS
    )
    # =========================================================================

    l1a_2_geo >> make_l1b
    make_l1b >> l2gen
    l1a_2_geo >> l2gen

    imars_etl_builder.add_tasks(
        this_dag, "product_id=5", [l1a_2_geo], [l2gen],
        ["myd0_otis_l2"], TMP_DIR,
        common_load_params={
            "json":'{"status_id":3, "area_id":2}'
        }
    )



# # TODO: these too...
# # === FGBNMS
# fgb_dag = DAG(
#     dag_id="fgbnms_modis_aqua_granule",
#     default_args=DEF_ARGS,
#     schedule_interval=SCHEDULE_INTERVAL
# )
#
# add_tasks(
#     fgb_dag,
#     region=fgbnms,
#     parfile=os.path.join(
#         os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/fgbnms/
#         "moda_l2gen.par"
#     )
# )
#
# # === A01
# ao1_dag = DAG(
#     dag_id="ao1_modis_aqua_granule",
#     default_args=DEF_ARGS,
#     schedule_interval=SCHEDULE_INTERVAL
# )
#
# add_tasks(
#     ao1_dag,
#     region=ao1,
#     parfile=os.path.join(
#         os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/ao1/
#         "moda_l2gen.par"
#     )
# )
#
# create_granule_l3 = BashOperator(
#     task_id="l3gen_granule",
#     bash_command="""
#         /opt/snap/bin/gpt {{params.gpt_xml_file}} \
#         -t {{ params.satfilename.l3_pass(execution_date, params.roi_place_name) }} \
#         -f NetCDF-BEAM \
#         `{{ params.satfilename.l2(execution_date, params.roi_place_name) }}`
#     """,
#     params={
#         'satfilename': satfilename,
#         'roi_place_name': ao1.place_name,
#         'gpt_xml_file': os.path.join(
#             os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/ao1/
#             "moda_l3g.par"
#         )
#     },
#     queue=QUEUE.SNAP,
#     dag = ao1_dag
# )
#
# # TODO: export tiff from l3
