from airflow.operators.bash_operator import BashOperator

from imars_dags.util.globals import QUEUE

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
myd03_modis_oc_l2gen = BashOperator(
    task_id='myd03_modis_oc_l2gen',
    bash_command="""/opt/ocssw/bin/l2gen
        ifile=$DATA_DIR/l1b/$FILENAME.L1B_LAC
        ofile=$DATA_DIR/L2_gen/$FILENAME.L2
        geofile={{params.root_path}}{{ params.pathbuilder(execution_date, "Y") }}
        par=$DATA_DIR/generic_l2gen.par
    """,
    params=myd03_params,
    dag=modis_aqua_processing,
    queue=QUEUE.SAT_SCRIPTS
)
