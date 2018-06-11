"""
manually triggered dag that runs processing for one modis pass
"""
# std libs
from datetime import datetime
import os

# deps
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG

# this package
from imars_dags.util.etl_tools.tmp_file import tmp_filepath
from imars_dags.util.etl_tools.extract import add_extract
from imars_dags.util.etl_tools.load import add_load
from imars_dags.util.etl_tools.cleanup import add_cleanup
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS

DEF_ARGS = DEFAULT_ARGS.copy()
DEF_ARGS.update({
    'start_date': datetime.utcnow(),
})

SCHEDULE_INTERVAL = None
AREA_SHORT_NAME = "gom"
PARFILE = os.path.join(
    os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/gom/
    "moda_l2gen.par"
)
# NOTE: xcalfile must be set for v7.4 and will need to be updated ~ 1/mo
#     for more info see:
#     https://oceancolor.gsfc.nasa.gov/forum/oceancolor/topic_show.pl?pid=37506
# TODO: (I think) this can be removed now that all nodes are v7.5+
XCALFILE = "$OCVARROOT/modisa/xcal/OPER/xcal_modisa_axc_oc_v1.12d"

this_dag = DAG(
    dag_id="proc_myd01_to_myd0_otis_l2_"+AREA_SHORT_NAME,
    default_args=DEF_ARGS,
    schedule_interval=SCHEDULE_INTERVAL
)

with this_dag as dag:
    # === EXTRACT INPUT FILES ===
    # ===========================================================================
    MYD01FILE = tmp_filepath(this_dag.dag_id, 'myd01.hdf')
    extract_myd01 = add_extract(this_dag, "product_id=5", MYD01FILE)

    # =========================================================================
    # === modis GEO
    # =========================================================================
    GEOFILE = tmp_filepath(dag.dag_id, 'geofile')
    l1a_2_geo = BashOperator(
        task_id='l1a_2_geo',
        bash_command="""
            export OCSSWROOT=/opt/ocssw      && \n\
            source /opt/ocssw/OCSSW_bash.env && \n\
            OUT_PATH="""+GEOFILE+"""         && \n\
            $OCSSWROOT/scripts/modis_GEO.py \\\n\
                --output=$OUT_PATH \\\n\
                """ + MYD01FILE + """ && \n\
            [[ -s $OUT_PATH ]]
        """,
        queue=QUEUE.SAT_SCRIPTS,
        trigger_rule=TriggerRule.ONE_SUCCESS  # run if any upstream passes
    )
    # =========================================================================

    # TODO: insert day/night check branch operator here?
    #       else ocssw will run on night granules too

    # =========================================================================
    # === modis l1a + geo -> l1b
    # =========================================================================
    OKMFILE = tmp_filepath(dag.dag_id, 'okm')  # aka L1b
    HKMFILE = tmp_filepath(dag.dag_id, 'hkm')
    QKMFILE = tmp_filepath(dag.dag_id, 'qkm')

    # NOTE: we need write access to the input file
    #       [ref](https://oceancolor.gsfc.nasa.gov/forum/oceancolor/topic_show.pl?tid=5333)
    #       This is because "MODIS geolocation updates L1A metadata for
    #       geographic coverage and orbital parameters"
    make_l1b = BashOperator(
        task_id='make_l1b',
        bash_command="""
            export OCSSWROOT=/opt/ocssw && \n\
            source /opt/ocssw/OCSSW_bash.env && \n\
            OKM_PATH="""+OKMFILE+""" && \n\
            HKM_PATH="""+HKMFILE+""" && \n\
            QKM_PATH="""+QKMFILE+""" && \n\
            $OCSSWROOT/scripts/modis_L1B.py \\\n\
                --okm $OKM_PATH \\\n\
                --hkm $HKM_PATH \\\n\
                --qkm $QKM_PATH \\\n\
                """+MYD01FILE+""" \\\n\
                """+GEOFILE+""" && \n\
            [[ -s $OKM_PATH ]]
        """,  # NOTE: might want to add `&& -s $HKM_PATH && -s $QKM_PATH` too
        queue=QUEUE.SAT_SCRIPTS
    )
    # =========================================================================
    # =========================================================================
    # === l2gen l1b -> l2
    # =========================================================================
    L2FILE = tmp_filepath(dag.dag_id, 'l2')
    # l2gen usage usage docs:
    # https://seadas.gsfc.nasa.gov/help/seadas-processing/ProcessL2gen.html#COMMAND_LINE_HELP
    # NOTE: filenames must be inserted inline here because they contain the
    #       airflow macro `{{ts_nodash}}`. If passed as params the macro does
    #       not get rendered resulting in a literal `{{ts_nodash}}` in the str.
    l2gen = BashOperator(
        task_id="l2gen",
        bash_command="""
            export OCSSWROOT=/opt/ocssw && \n\
            source /opt/ocssw/OCSSW_bash.env && \n\
            $OCSSWROOT/bin/l2gen \\\n\
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
    load_l2_list = add_load(
        this_dag,
        to_load=[
            {
                "filepath": L2FILE,  # required!
                "verbose": 3,
                "product_id": 35,
                # "time":"2016-02-12T16:25:18",
                # "datetime": datetime(2016,2,12,16,25,18),
                "json": '{'
                    '"status_id":3,'  # noqa E131
                    '"area_id":1,'
                    '"area_short_name":"' + AREA_SHORT_NAME + '"'
                '}'
            }
        ],
        upstream_operators=[l2gen]
    )

    cleanup_task = add_cleanup(
        this_dag,
        to_cleanup=[MYD01FILE, GEOFILE, OKMFILE, HKMFILE, QKMFILE, L2FILE],
        upstream_operators=load_l2_list
    )

    # =========================================================================
    # === connect it all up
    # =========================================================================
    extract_myd01 >> l1a_2_geo
    l1a_2_geo >> make_l1b
    make_l1b >> l2gen
    l1a_2_geo >> l2gen
    # `l2gen >> load_l2_list` done by `upstream_operators=[l2gen]`
    # `load_l2_list >> cleanup_task` via `upstream_operators=[load_l2_list]`
    # =========================================================================

# # TODO: these too...
#from imars_dags.regions import gom, fgbnms, ao1
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
# from imars_dags.util import satfilename
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
