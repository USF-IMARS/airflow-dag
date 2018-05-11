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

def add_tasks(dag, region, parfile):
    with dag as dag:
        # =========================================================================
        # === extract granule l1a data
        # =========================================================================
        # === option 1: l1a file already exists so we have nothing to do
        check_for_extant_l1a_file = BashOperator(
            task_id='check_for_extant_l1a_file',
            bash_command='[[ -s {{ params.filepather.myd01(execution_date, params.roi) }} ]]',
            params={
                'filepather': satfilename,
                'roi': region.place_name
            },
            retries=1,
            retry_delay=timedelta(seconds=3)
        )
        # === option 2: extract bz2 file from our local archive
        extract_l1a_bz2 = BashOperator(
            task_id='extract_l1a_bz2',
            bash_command="""
                bzip2 -k -c -d {{ params.filepather.l1a_lac_hdf_bz2(execution_date, params.roi) }} > {{ params.filepather.myd01(execution_date, params.roi) }}
            """,
            params={
                'filepather': satfilename,
                'roi': region.place_name
            },
            trigger_rule=TriggerRule.ALL_FAILED,  # only run if upstream fails
            retries=1,
            retry_delay=timedelta(seconds=5)
        )
        check_for_extant_l1a_file >> extract_l1a_bz2
        # === option 3: download the granule
        # reads the download url from a metadata file created in the last step and
        # downloads the file iff the file does not already exist.
        download_granule = BashOperator(
            task_id='download_granule',
            bash_command="""
                METADATA_FILE={{ params.filepather.metadata(execution_date, params.roi) }} &&
                OUT_PATH={{ params.filepather.myd01(execution_date, params.roi) }}         &&
                FILE_URL=$(grep "^upstream_download_link" $METADATA_FILE | cut -d'=' -f2-) &&
                [[ -s $OUT_PATH ]] &&
                echo "file already exists; skipping download." ||
                curl --user {{params.username}}:{{params.password}} -f $FILE_URL -o $OUT_PATH
                && [[ -s $OUT_PATH ]]
            """,
            params={
                "filepather": satfilename,
                "username": secrets.ESDIS_USER,
                "password": secrets.ESDIS_PASS,
                "roi": region.place_name
            },
            trigger_rule=TriggerRule.ALL_FAILED  # only run if upstream fails
        )
        extract_l1a_bz2 >> download_granule
        # =========================================================================
        # =========================================================================
        # === modis GEO
        # =========================================================================
        l1a_2_geo = BashOperator(
            task_id='l1a_2_geo',
            bash_command="""
                export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
                OUT_PATH={{ params.geo_pather(execution_date, params.roi) }} && \
                /opt/ocssw/run/scripts/modis_GEO.py \
                --output=$OUT_PATH \
                {{params.l1a_pather(execution_date, params.roi)}} \
                && [[ -s $OUT_PATH ]]
            """,
            params={
                'l1a_pather': satfilename.myd01,
                'geo_pather': satfilename.l1a_geo,
                'roi': region.place_name
            },
            queue=QUEUE.SAT_SCRIPTS,
            trigger_rule=TriggerRule.ONE_SUCCESS  # run if any upstream passes
        )
        check_for_extant_l1a_file >> l1a_2_geo
        extract_l1a_bz2           >> l1a_2_geo
        download_granule          >> l1a_2_geo
        # =========================================================================

        # TODO: insert day/night check branch operator here? else ocssw will run on night granules too

        # =========================================================================
        # === modis l1a + geo -> l1b
        # =========================================================================
        make_l1b = BashOperator(
            task_id='make_l1b',
            bash_command="""
                export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
                OKM_PATH={{params.okm_pather(execution_date, params.roi)}} && \
                HKM_PATH={{params.hkm_pather(execution_date, params.roi)}} && \
                QKM_PATH={{params.qkm_pather(execution_date, params.roi)}} && \
                $OCSSWROOT/run/scripts/modis_L1B.py \
                --okm=$OKM_PATH \
                --hkm=$HKM_PATH \
                --qkm=$QKM_PATH \
                {{params.l1a_pather(execution_date, params.roi)}} \
                {{params.geo_pather(execution_date, params.roi)}} \
                && [[ -s $OKM_PATH && -s $HKM_PATH && -s $QKM_PATH ]]
            """,
            params={
                'l1a_pather': satfilename.myd01,
                'geo_pather': satfilename.l1a_geo,
                'okm_pather': satfilename.okm,
                'hkm_pather': satfilename.hkm,
                'qkm_pather': satfilename.qkm,
                'roi': region.place_name
            },
            queue=QUEUE.SAT_SCRIPTS
        )
        l1a_2_geo >> make_l1b
        ## these are true, but kind of superfluous
        # check_for_extant_l1a_file >> make_l1b
        # extract_l1a_bz2           >> make_l1b
        # download_granule          >> make_l1b
        # =========================================================================
        # =========================================================================
        # === l2gen l1b -> l2
        # =========================================================================
        l2gen = BashOperator(
            task_id="l2gen",
            bash_command="""
                export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
                $OCSSWROOT/run/bin/linux_64/l2gen \
                ifile={{params.l1b_pather(execution_date, params.roi)}} \
                ofile={{params.l2_pather(execution_date, params.roi)}} \
                geofile={{params.geo_pather(execution_date, params.roi)}} \
                par={{params.parfile}}
            """,
            params={
                'l1b_pather': satfilename.okm,
                'geo_pather': satfilename.l1a_geo,
                'l2_pather':  satfilename.l2,
                'parfile': parfile,
                'roi': region.place_name
            },
            queue=QUEUE.SAT_SCRIPTS
        )
        make_l1b >> l2gen
        l1a_2_geo >> l2gen
        # =========================================================================


# === GOM
gom_dag = DAG(
    dag_id="gom_modis_aqua_granule",
    default_args=DEF_ARGS,
    schedule_interval=SCHEDULE_INTERVAL
)

add_tasks(
    gom_dag,
    region=gom,
    parfile=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/gom/
        "moda_l2gen.par"
    )
)

# === FGBNMS
fgb_dag = DAG(
    dag_id="fgbnms_modis_aqua_granule",
    default_args=DEF_ARGS,
    schedule_interval=SCHEDULE_INTERVAL
)

add_tasks(
    fgb_dag,
    region=fgbnms,
    parfile=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/fgbnms/
        "moda_l2gen.par"
    )
)

# === A01
ao1_dag = DAG(
    dag_id="ao1_modis_aqua_granule",
    default_args=DEF_ARGS,
    schedule_interval=SCHEDULE_INTERVAL
)

add_tasks(
    ao1_dag,
    region=ao1,
    parfile=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/ao1/
        "moda_l2gen.par"
    )
)

create_granule_l3 = BashOperator(
    task_id="l3gen_granule",
    bash_command="""
        /opt/snap/bin/gpt {{params.gpt_xml_file}} \
        -t {{ params.satfilename.l3_pass(execution_date, params.roi_place_name) }} \
        -f NetCDF-BEAM \
        `{{ params.satfilename.l2(execution_date, params.roi_place_name) }}`
    """,
    params={
        'satfilename': satfilename,
        'roi_place_name': ao1.place_name,
        'gpt_xml_file': os.path.join(
            os.path.dirname(os.path.realpath(__file__)),  # imars_dags/dags/ao1/
            "moda_l3g.par"
        )
    },
    queue=QUEUE.SNAP,
    dag = ao1_dag
)

# TODO: export tiff from l3
