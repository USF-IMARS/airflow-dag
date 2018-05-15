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
# NOTE: xcalfile must be set for v7.4 and will need to be updated ~ 1/mo
#     for more info see:
#     https://oceancolor.gsfc.nasa.gov/forum/oceancolor/topic_show.pl?pid=37506
XCALFILE="$OCVARROOT/modisa/xcal/OPER/xcal_modisa_axc_oc_v1.12d"

this_dag = DAG(
    dag_id="gom_myd01_to_myd0_otis_l2",
    default_args=DEF_ARGS,
    schedule_interval=SCHEDULE_INTERVAL
)

with this_dag as dag:
    # =========================================================================
    # === modis GEO
    # =========================================================================
    GEOFILE = imars_etl_builder.tmp_filepath(dag.dag_id, 'geofile')
    l1a_2_geo = BashOperator(
        task_id='l1a_2_geo',
        bash_command="""
            export OCSSWROOT=/opt/ocssw      && \n\
            source /opt/ocssw/OCSSW_bash.env && \n\
            OUT_PATH="""+GEOFILE+"""         && \n\
            /opt/ocssw/run/scripts/modis_GEO.py \\\n\
                --output=$OUT_PATH \\\n\
                {{ ti.xcom_pull(task_ids="extract_file") }} && \n\
            [[ -s $OUT_PATH ]]
        """,
        queue=QUEUE.SAT_SCRIPTS,
        trigger_rule=TriggerRule.ONE_SUCCESS  # run if any upstream passes
    )
    # =========================================================================

    # TODO: insert day/night check branch operator here? else ocssw will run on night granules too

    # =========================================================================
    # === modis l1a + geo -> l1b
    # =========================================================================
    OKMFILE = imars_etl_builder.tmp_filepath(dag.dag_id, 'okm')  # aka L1b
    HKMFILE = imars_etl_builder.tmp_filepath(dag.dag_id, 'hkm')
    QKMFILE = imars_etl_builder.tmp_filepath(dag.dag_id, 'qkm')

    # NOTE: we need write access to the input file
    #       [ref](https://oceancolor.gsfc.nasa.gov/forum/oceancolor/topic_show.pl?tid=5333)
    #       This is because "MODIS geolocation updates L1A metadata for
    #       geographic coverage and orbital parameters"
    make_l1b = BashOperator(
        task_id='make_l1b',
        bash_command="""
            export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \n\
            OKM_PATH="""+OKMFILE+""" && \n\
            HKM_PATH="""+HKMFILE+""" && \n\
            QKM_PATH="""+QKMFILE+""" && \n\
            $OCSSWROOT/run/scripts/modis_L1B.py \\\n\
                --okm $OKM_PATH \\\n\
                --hkm $HKM_PATH \\\n\
                --qkm $QKM_PATH \\\n\
                {{ ti.xcom_pull(task_ids="extract_file") }} \\\n\
                """+GEOFILE+""" && \n\
            [[ -s $OKM_PATH ]]
        """,  # NOTE: might want to add `&& -s $HKM_PATH && -s $QKM_PATH` too
        queue=QUEUE.SAT_SCRIPTS
    )
    # =========================================================================
    # =========================================================================
    # === l2gen l1b -> l2
    # =========================================================================
    L2FILE = imars_etl_builder.tmp_filepath(dag.dag_id, 'l2')
    l2gen = BashOperator(
        task_id="l2gen",
        bash_command="""
            export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \n\
            $OCSSWROOT/run/bin/linux_64/l2gen \\\n\
                ifile="""+OKMFILE+""" \\\n\
                ofile="""+L2FILE+""" \\\n\
                geofile="""+GEOFILE+""" \\\n\
                xcalfile={{params.xcalfile}}\\\n\
                par={{params.parfile}} && \n\
            [[ -s """+L2FILE+""" ]]
        """,
        params={
            'parfile': PARFILE,
            'xcalfile': XCALFILE
        },
        queue=QUEUE.SAT_SCRIPTS
    )
    # =========================================================================

    l1a_2_geo >> make_l1b
    make_l1b >> l2gen
    l1a_2_geo >> l2gen

    imars_etl_builder.add_tasks(
        this_dag,
        sql_selector="product_id=5",
        first_transform_operators=[l1a_2_geo],
        last_transform_operators=[l2gen],
        files_to_load=[
            {
                "filepath":L2FILE,  # required!
                "verbose":3,
                "product_id":5,
                # "time":"2016-02-12T16:25:18",
                # "datetime": datetime(2016,2,12,16,25,18),
                "json":'{"status_id":3,"area_id":2}'
            }
        ],
        to_cleanup=[GEOFILE,OKMFILE,HKMFILE,QKMFILE,L2FILE]
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
