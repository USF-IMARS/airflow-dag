from airflow.operators.bash_operator import BashOperator

from imars_dags.util import satfilename

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
