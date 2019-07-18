"""
This is a ingest DAG for Sentinel 3 OLI products.
"""
# =====================================================================
# === imports
# Python imports should always be at the top of the file.
# You probably don't need to worry about these much and will
# just copy-paste them into your DAG.
# =====================================================================
# python std librarys
from datetime import datetime
import os  

# non-std python librarys
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# this package
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.get_default_args import get_default_args
# =====================================================================
# =====================================================================
# === Area ID constants
# These constants come from the imars_product_metadata database.
# The region ID number uniquely identifies a spatial area.
#
# You may find (and add) product IDs to the SQL database using this file:
# https://github.com/USF-IMARS/imars_puppet/blob/test/modules/role/files/sql/product_metadata_rows.sql
#
# The region ID and name must match values in the sql file.
# =====================================================================

AREA_ID = 12
AREA_SHORT_NAME = "florida"
L1_PRODUCT_ID = 36

# =====================================================================
# =====================================================================
# === Initial DAG Setup
# In this section you will need to:
#    1. modify `DAG_NAME`
#    2. set the `start_date` to the earliest date of the data you might
#       want to ingest.
#    3. set keyword arguments like `schedule_interval` for the DAG init
#       as desired based on your coverage check strategy
#       (see `./doc/ingest_dags/coverage_checks.md`)
# =====================================================================
# DAG_NAME should be same as the filename (without .py extention).

DAG_NAME = "s3_api_ingest"
DAG_ID = get_dag_id(
    __file__, region=AREA_SHORT_NAME, dag_name=DAG_NAME
)

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

this_dag = DAG(
    dag_id=DAG_ID,
    # KEEP ONLY ONE SECTION BELOW:
    # === 1) for granule-datematched coverage checks
    default_args=get_default_args(
        # this start date should be the first granule to ingest
        start_date=datetime(2016, 2, 16, 1, 00, 00)
    ),
    schedule_interval=None,  # the frequency of granules
    catchup=True,  # latest only
    # ===
    # === 2) for latest-only coverage checks:
    #schedule_interval=timedelta(minutes=5),  # how often to check       #TODO once have all images up to today's date, make the check once a day or week
    #catchup=False,  # latest only
    #max_active_runs=1,
    # ===
)
this_dag.doc_md = __doc__  # sets web GUI to use docstring at top of file

# =====================================================================
# =====================================================================
# === Defining the first operator.
# Operators are how you define a type of "job".
# Here we create a BashOperator because we want to run the
# bash commands in the bash script
# `./doc/ingest_dags/scripts/example_ingest_script.sh`.
# =====================================================================
# the variable name for the operator (run_the_script) should match
# the "task_id".

s3_api_query_metadata = PythonOperator(
    task_id='s3_api',
    python_callable="s3_api.py",
    params={
        "region_name": AREA_SHORT_NAME,
        "area_id": AREA_ID,
        "florida_geojson" : os.path.join(
            THIS_DIR,
            "florida.geojson"
        ),
        "metadata_s3" : os.path.join(
            THIS_DIR,
            "metadata_s3.json"
        )
    },
    dag=this_dag,
)
#s3_api_download = PythonOperator(
#    task_id='s3_api_load',
#    python_command="s3_api_load.py",
#    params={
#        "region_name": AREA_SHORT_NAME,
#        "area_id": AREA_ID,
#    },
#    dag=this_dag,
#)

#s3_api_query_metadata >> s3_api_download

globals()[this_dag.dag_id] = this_dag
