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
# === unzip the files
# =============================================================================
# obdaac_ingest_unzip = BashOperator(
#     task_id='obdaac_ingest_unzip',
#     bash_command="""
#         bzip2 -d -k -c {{ params.bz2_pather(execution_date) }} > {{ params.l1a_pather(execution_date) }}
#     """,
#     params={
#         'bz2_pather': satfilename.l1a_LAC_bz2,
#         'l1a_pather': satfilename.l1a_LAC
#     },
#     dag=modis_aqua_processing
# )
# obdaac_ingest_filecheck >> obdaac_ingest_unzip
# =============================================================================
