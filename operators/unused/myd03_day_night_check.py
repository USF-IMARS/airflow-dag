from airflow.operators.bash_operator import BashOperator

from imars_dags.util.globals import QUEUE
from imars_dags.util import satfilename

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
#     params={
#         'root_path':'/TODO/update/this/',
#         'pathbuilder': satfilename.myd03_day_night
#     },
#     dag=this_dag,
#     queue=QUEUE.SAT_SCRIPTS
# )
# myd03_filecheck >> myd03_day_night
# =============================================================================
