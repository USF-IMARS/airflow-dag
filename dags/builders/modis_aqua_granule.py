"""
manually triggered dag that runs processing for one modis pass
"""
# std libs
from datetime import datetime, timedelta
import subprocess
import configparser

# deps
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# this package
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS
from imars_dags.util import satfilename
from imars_dags.settings import secrets  # NOTE: this file not in public repo!

default_args = DEFAULT_ARGS.copy()
default_args.update({
    'start_date': datetime.utcnow(),
    'retries': 1
})

schedule_interval=None

def add_tasks(dag, region, parfile):
    with dag as dag:
        # =========================================================================
        # === extract granule l1a data
        # =========================================================================
        # === option 1: l1a file already exists so we have nothing to do
        check_for_extant_l1a_file = BashOperator(
            task_id='check_for_extant_l1a_file',
            bash_command='[[ -f {{ params.filepather.myd01(execution_date, params.roi) }} ]]',
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
                bzip2 -k -d {{ params.filepather.l1a_lac_hdf_bz2(execution_date, params.roi) }} {{ params.filepather.myd01(execution_date, params.roi) }}
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
                [[ -f $OUT_PATH ]] &&
                echo "file already exists; skipping download." ||
                curl --user {{params.username}}:{{params.password}} -f $FILE_URL -o $OUT_PATH
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
                /opt/ocssw/run/scripts/modis_GEO.py \
                --output={{params.geo_pather(execution_date, params.roi)}} \
                {{params.l1a_pather(execution_date, params.roi)}}
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
                $OCSSWROOT/run/scripts/modis_L1B.py \
                --okm={{params.okm_pather(execution_date, params.roi)}} \
                --hkm={{params.hkm_pather(execution_date, params.roi)}} \
                --qkm={{params.qkm_pather(execution_date, params.roi)}} \
                {{params.l1a_pather(execution_date, params.roi)}} \
                {{params.geo_pather(execution_date, params.roi)}}
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
