from airflow.operators.bash_operator import BashOperator

from imars_dags.util import satfilename

# =============================================================================
# === file existance check
# =============================================================================
# test -e gives exit status 1 if file DNE
# This node will fail if the input files have not been ingested and thus the DAG
# will not run. This is useful because we run every 5m, but we really only want
# to process the granules that have come in from our subscription. When the
# DAG fails at this stage, then you know that the granule for this time was
# not ingested by the subscription service.
# obdaac_ingest_filecheck = BashOperator(
#     task_id='obdaac_ingest_filecheck',
#     bash_command="""
#         test -e {{ params.satfilepather(execution_date) }}
#     """,
#     params={
#        'satfilepather': satfilename.l1a_LAC_bz2,
#     },
#     dag=this_dag,
#     retries=0
# )
# wait_for_data_delay >> obdaac_ingest_filecheck
# =============================================================================
