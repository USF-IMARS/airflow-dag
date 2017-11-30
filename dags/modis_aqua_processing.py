"""
airflow processing pipeline definition for MODIS aqua data
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

# === ./imars_dags/modis_aqua_processing.py :
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, POOL
from imars_dags.util import satfilename
from imars_dags.settings.regions import REGIONS

# for each (new) pass file:
modis_aqua_processing = DAG(
    'modis_aqua_processing',
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(minutes=5)
)

# =============================================================================
# === file existance check
# =============================================================================
# test -e gives exit status 1 if file DNE
# This node will fail if the input files have not been ingested and thus the DAG
# will not run. This is useful because we run every 5m, but we really only want
# to process the granules that have come in from our subscription. When the
# DAG fails at this stage, then you know that the granule for this time was
# not ingested by the subscription service.
obdaac_ingest_filecheck = BashOperator(
    task_id='obdaac_ingest_filecheck',
    bash_command="""
        test -e {{ params.satfilepather(execution_date) }}
    """,
    params={
       'satfilepather': satfilename.l1a_LAC_bz2,
    },
    dag=modis_aqua_processing
)
# =============================================================================
# =============================================================================
# === unzip the files
# =============================================================================
obdaac_ingest_unzip = BashOperator(
    task_id='obdaac_ingest_unzip',
    bash_command="""
        bzip2 -d -k -c {{ params.bz2_pather(execution_date) }} > {{ params.l1a_pather(execution_date) }}
    """,
    params={
        'bz2_pather': satfilename.l1a_LAC_bz2,
        'l1a_pather': satfilename.l1a_LAC
    },
    dag=modis_aqua_processing
)
obdaac_ingest_filecheck >> obdaac_ingest_unzip
# =============================================================================
# =============================================================================
# === modis GEO
# =============================================================================
l1a_2_geo = BashOperator(
    task_id='l1a_2_geo',
    bash_command="""
        export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
        /opt/ocssw/run/scripts/modis_GEO.py \
        --output={{params.geo_pather(execution_date)}} \
        {{params.l1a_pather(execution_date)}}
    """,
    params={
        'l1a_pather': satfilename.l1a_LAC,
        'geo_pather': satfilename.l1a_geo
    },
    dag=modis_aqua_processing
)
obdaac_ingest_unzip >> l1a_2_geo
# =============================================================================
# === modis l1a + geo -> l1b
# =============================================================================
make_l1b = BashOperator(
    task_id='make_l1b',
    bash_command="""
        export OCSSWROOT=/opt/ocssw && source /opt/ocssw/OCSSW_bash.env && \
        $OCSSWROOT/run/scripts/modis_L1B.py \
        --okm={{params.okm_pather(execution_date)}} \
        --hkm={{params.okm_pather(execution_date)}} \
        --qkm={{params.okm_pather(execution_date)}} \
        {{params.l1a_pather(execution_date)}} \
        {{params.geo_pather(execution_date)}}
    """,
    params={
        'l1a_pather': satfilename.l1a_LAC,
        'geo_pather': satfilename.l1a_geo,
        'okm_pather': satfilename.okm,
        'hkm_pather': satfilename.hkm,
        'qkm_pather': satfilename.qkm
    },
    dag=modis_aqua_processing
)
l1a_2_geo >> make_l1b
obdaac_ingest_unzip >> make_l1b
# =============================================================================
# =============================================================================
# === Check Day/Night Metadata for given pass mxd03 file
# =============================================================================
# this node will fail if the input file is a night pass and the DAG will not
# proceed.
#
# Ported from [SeaDAS IPOPP SPA](https://github.com/USF-IMARS/seadas_spa)
#     * SPA=seadas
#     * station=modis_oc [cfgfile](https://github.com/USF-IMARS/seadas_spa/blob/master/station/modis_oc/station.cfgfile)
#     * algorithm=oc
#         * [generic.xml](https://github.com/USF-IMARS/seadas_spa/blob/master/wrapper/oc/generic.xml)
#         * [installation.xml](https://github.com/USF-IMARS/seadas_spa/blob/master/wrapper/oc/installation.xml)
#     * executable=DayNight
#
#   Ncs_run.cmd='{DayNight} {geo_file}'
#      standardFile="stdfile_DayNightCheck"
#      errorFile="errfile_DayNightCheck">
#      <env name="MODIS_DB_HOME" value="{algohome}" />

# myd03_day_night = BashOperator(
#     task_id='myd03_day_night',
#     bash_command="""/opt/sat-scripts/sat-scripts/DayNight.sh
#         {{params.root_path}}{{ params.pathbuilder(execution_date, "Y") }}
#     """,
#     params=myd03_params,
#     dag=modis_aqua_processing,
#     queue=QUEUE.SAT_SCRIPTS
# )
# myd03_filecheck >> myd03_day_night
# =============================================================================
# =============================================================================
# === seadas modis_oc
# =============================================================================
# Ported from [SeaDAS IPOPP SPA](https://github.com/USF-IMARS/seadas_spa)
#     * SPA=seadas
#     * station=modis_oc [cfgfile](https://github.com/USF-IMARS/seadas_spa/blob/master/station/modis_oc/station.cfgfile)
#     * algorithm=oc
#         * [generic.xml](https://github.com/USF-IMARS/seadas_spa/blob/master/wrapper/oc/generic.xml)
#         * [installation.xml](https://github.com/USF-IMARS/seadas_spa/blob/master/wrapper/oc/installation.xml)
#     * executable=l2gen
#
#   Ncs_run.cmd="{l2gen}  {metflag} {oisstflag} {ozoneflag} {seaiceflag} l2prod={products} ifile={data_file}
#       geofile={geo_file} ofile={output_file} {calflag}"
#         standardFile="stdfile_l2gen"
#         errorFile="errfile_l2gen">
#        <env name="OCDATAROOT" value="{algohome}{/}run{/}data" />
#        <env name="OCSSWROOT" value="{algohome}" />
#        <env name="OCVARROOT" value="{algohome}{/}run{/}var" />
#        <env name="PATH" value=".:{algohome}{/}run{/}bin{/}linux:${PATH}" />
# myd03_modis_oc_l2gen = BashOperator(
#     task_id='myd03_modis_oc_l2gen',
#     bash_command="""/opt/ocssw/bin/l2gen
#         ifile=$DATA_DIR/l1b/$FILENAME.L1B_LAC
#         ofile=$DATA_DIR/L2_gen/$FILENAME.L2
#         geofile={{params.root_path}}{{ params.pathbuilder(execution_date, "Y") }}
#         par=$DATA_DIR/generic_l2gen.par
#     """,
#     params=myd03_params,
#     dag=modis_aqua_processing,
#     queue=QUEUE.SAT_SCRIPTS
# )


# =============================================================================
# =============================================================================
# === IMaRS chlor_a oc_png cut_mapped_pass PngGenerator
# =============================================================================
# Ported from [IMaRS IPOPP SPA](https://github.com/USF-IMARS/imars)
#     * SPA=imars
#     * station=oc_png station [cfgfile](https://github.com/USF-IMARS/imars/blob/master/station/oc_png/station.cfgfile)
#     * algorithm=cut_mapped_pass
#         * [generic.xml](https://github.com/USF-IMARS/imars/blob/master/wrapper/cut_mapped_pass/generic.xml)
#         * [installation.xml](https://github.com/USF-IMARS/imars/blob/master/wrapper/cut_mapped_pass/installation.xml)
#
# Ncs_run.cmd="{generate_thumbnail} -f {input_file} -m {{mask_file}}
#   -c {conversion} -o {output_file} -s {sds} -n {no_data} -l {valid_min}
#   -u {valid_max} {coordinates}"
#
oc_png_template = """
    /opt/sat-scripts/sat-scripts/PngGenerator.py
        -f {{params.root_path}}{{ params.pathbuilder(execution_date, "Y") }}
        -m {params.product}_{params.place_name}_{params.sensor}mask
        -c {params.conversion}
        -o /srv/imars-objects/region_{params.place_name}/l3_modis_chlor_a_1km_pass_png/{params.sat}.{{execution_date}}.{params.place_name}.{params.sds}.png
        -s {params.sds}
        -n {params.no_data}
        -l {params.valid_min}
        -u {params.valid_max}
        {params.coordinates}
"""

# construct one of these tasks for each region:
# for region in REGIONS:
#     oc_png_region = BashOperator(
#         task_id='oc_png_'+region.place_name,
#         bash_command=oc_png_template,
#         params={
#             'root_path': myd03_params.root_path,
#             'pathbuilder': myd03_params.pathbuilder,
#             'product':"chlor_a",
#             'sds': "chlor_a",
#             'sat': 'aqua',
#             'place_name':region.place_name,
#             'sensor':'modis',
#             # This is a python equation for each thumbnails.
#             #   DO NOT PUT SPACES INSIDE THE EQUATION, but separate equations
#             #   with spaces. Input must be data.
#             'conversion': 'np.log10(data+1)/0.00519 250*np.log10((0.59*(data*5)**.86)+1.025)/np.log10(2)',
#             'no_data': 0, # seadas raw value for no data 0 for hdf5, was -32767
#             'valid_min': 'NaN',
#             'valid_max': 'NaN',
#             'coordinates': # TODO: something like the following from the generic.xml:
#             # "-a {source_north} -d {source_east} -e {source_south} -g {source_west} -w {latmax} -x {lonmax} -y {latmin} -z {lonmin}"
#         },
#         dag=modis_aqua_processing,
#         queue=QUEUE.SAT_SCRIPTS
#     )
#     # TODO: set imars.{sat}.{sensor}.{product_family}.mapped as upstream
#     myd03_day_night >> oc_png_region
# =============================================================================

# TODO: very similar to above, but with nflh instead of chlor_a
