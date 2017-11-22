"""
airflow processing pipeline definition for MODIS data
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

# === ./imars_dags/modis_aqua_processing.py :
from imars_dags.util.globals import QUEUE, DEFAULT_ARGS, POOL


# for each (new) pass file:
modis_aqua_processing = DAG('modis_aqua_processing', default_args=DEFAULT_ARGS, schedule_interval=timedelta(minutes=5))


# =============================================================================
# === file existance check
# =============================================================================
# test -e gives exit status 1 if file DNE
# This node will fail if the input files have not been ingested and thus the DAG
# will not run. This is useful because we run every 5m, but we really only want
# to process the granules that have come in from our subscription. When the
# DAG fails at this stage, then you know that the granule for this time was
# not ingested by the subscription service.

def myd03_filename(
    product_datetime,
    sat_char
):
    """ builds a filename for M*D03.YYDDDHHMMSS.hdf formatted paths.
    sat_char=Y for Aqua, O for Terra
    """
    return "M{}D03.{}.hdf".format(sat_char, product_datetime.strftime("%y%j%H%M%S"))

myd03_params = {
   'pathbuilder': myd03_filename,
   'root_path': "/srv/imars-objects/nrt-pub/data/aqua/modis/level1/",
}

myd03_filecheck = BashOperator(
    task_id='mYd03_filecheck',
    bash_command="""
        test -e {{params.root_path}}{{ params.pathbuilder(execution_date, "Y") }}
    """,
     params=myd03_params,
    dag=modis_aqua_processing
)
# =============================================================================
# =============================================================================
# === Day/Night Metadata for given pass mxd03 file
# =============================================================================
myd03_day_night = BashOperator(
    task_id='myd03_day_night',
    bash_command='/opt/sat-scripts/sat-scripts/DayNight.sh {{ params.pathbuilder(execution_date, "Y") }}',
    params=myd03_params,
    dag=modis_aqua_processing,
    queue=QUEUE.SAT_SCRIPTS,
    pool=POOL.SAT_SCRIPTS
)
myd03_filecheck >> myd03_day_night

# =============================================================================
# =============================================================================
# === IMaRS oc_png cut_mapped_pass PngGenerator
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
        -f {params.input_file}
        -m {params.mask_file}
        -c {params.conversion}
        -o {params.output_file}
        -s {params.sds}
        -n {params.no_data}
        -l {params.valid_min}
        -u {params.valid_max}
        {params.coordinates}
"""

# TODO: construct one of these tasks for each region:
# oc_png = BashOperator(
#     task_id='oc_png',
#     bash_command=oc_png_template,
#     params={
#         'input_file': '',
#         'output_file': '',
#         # TODO
#     },
#     dag=dag_processing
# )
# TODO: set imars.{sat}.{sensor}.{product_family}.mapped as upstream
# t3.set_upstream(t1)
# =============================================================================
