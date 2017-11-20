"""
airflow processing pipeline definition for MODIS data
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 7),
    'email': ['imarsroot@marine.usf.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=90),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag_ingest = DAG('modis', default_args=default_args, schedule_interval=timedelta(hours=6))

# =============================================================================
# === Modis ingest subscription(s)
# =============================================================================
modis_ingest = BashOperator(
    task_id='subscription_1310',
    bash_command='/opt/RemoteDownlinks/ingest_subscription.py',
    dag=dag_ingest
)
# NOTE: this writes files out to /srv/imars-objects/subscription-1310/modis_l0
# example filenames:
# MOD00.A2017318.0430_1.PDS.bz2
# MOD00.A2017309.0115_1.PDS.bz2
# =============================================================================

# for each (new) pass file:
dag_processing = DAG('modis_processing', default_args=default_args, schedule_interval=timedelta(minutes=5))


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
    """ builds a filename for M*D03.YYDDDHHMMSS.hdf formatted paths """
    return "M{}D03.{}.hdf".format(sat_char, product_datetime.strftime("%y%j%H%M%S"))

modis_processing_filecheck = BashOperator(
    task_id='filecheck',
    bash_command="""
        test -e {{params.root_path}}{{ params.pathbuilder(execution_date, "Y") }}
    """,
     params={
        'pathbuilder': myd03_filename,
        'root_path': "/srv/imars-objects/nrt-pub/data/aqua/modis/level1/",
    },
    dag=dag_processing
)
# =============================================================================
# =============================================================================
# === Day/Night Metadata for given pass mxd03 file
# =============================================================================
# new_mxd03_path=""
# modis_mxd03_day_night = BashOperator(
#     task_id='modis_mxd03_day_night',
#     bash_command='/opt/sat-scripts/sat-scripts/DayNight.sh '+ new_mxd03_path,
#     dag=dag_processing
# )

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
