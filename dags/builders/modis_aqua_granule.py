"""
manually triggered dag that runs processing for one modis pass
"""
# std libs
from datetime import datetime
import subprocess
import configparser

# deps
from airflow.operators.bash_operator import BashOperator

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
        # === download the granule
        # =========================================================================
        # reads the download url from a metadata file created in the last step and
        # downloads the file iff the file does not already exist.
        download_granule = BashOperator(
            task_id='download_granule',
            # trigger_rule='one_success',
            bash_command="""
                METADATA_FILE={{ params.filepather.metadata(execution_date, params.roi) }} &&
                OUT_PATH={{ params.filepather.myd01(execution_date, params.roi) }}         &&
                FILE_URL=$(grep "^upstream_download_link" $METADATA_FILE | cut -d'=' -f2-) &&
                [[ -f $OUT_PATH ]] &&
                echo "file already exists; skipping download." ||
                wget --user={{params.username}} --password={{params.password}} --tries=1 --no-verbose --output-document=$OUT_PATH $FILE_URL
            """,
            params={
                "filepather": satfilename,
                "username": secrets.ESDIS_USER,
                "password": secrets.ESDIS_PASS,
                "roi": region.place_name
            }
        )
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
            queue=QUEUE.SAT_SCRIPTS
        )
        download_granule >> l1a_2_geo
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
        download_granule >> make_l1b
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