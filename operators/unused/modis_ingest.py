from airflow.operators.bash_operator import BashOperator

# =============================================================================
# === Modis ingest subscription(s)
# =============================================================================
# subscription_1310 = BashOperator(
#     task_id='subscription_1310',
#     bash_command='/opt/RemoteDownlinks/ingest_subscription.py',
#     dag=modis_ingest
# )
# NOTE: this writes files out to /srv/imars-objects/subscription-1310/modis_l0
# example filenames:
# MOD00.A2017318.0430_1.PDS.bz2
# MOD00.A2017309.0115_1.PDS.bz2
# =============================================================================
