from airflow.operators.bash_operator import BashOperator

from imars_dags.util import satfilename

# def l1a_LAC(
#     product_datetime, region_id
# ):
#     """ DEPRECATED : myd01 should be used instead!
#         returns file path for unzipped modis aqua files (see also l1a_LAC_bz2)
#         I *think* these are myd01 files, same as myd01(), but from PO.DAAC.
#     """
#     return "/srv/imars-objects/modis_aqua_" + region_id + "/l1a/" + "A{}00.L1A_LAC".format(
#         product_datetime.strftime("%Y%j%H%M")
#     )

# def l1a_LAC_bz2(
#     product_datetime
# ):
#     """ DEPRECATED : myd01 should be used instead!
#     Gets file path for 1a aqua modis files zipped together from OB.DAAC.
#     """
#     base_path="/srv/imars-objects/homes/scratch/epa/satellite/modis/GOM/L2G_MODA_sub1973_day/"
#     return base_path+"A{}00.L1A_LAC.bz2".format(
#         product_datetime.strftime("%Y%j%H%M")
#     )

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
