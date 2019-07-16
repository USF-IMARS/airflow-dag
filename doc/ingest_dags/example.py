"""
This is an example IMaRS ingest DAG.
It is heavily commented and demonstrates a very basic usage.

This first section is called a docstring and should summarize the contents of
the file.
"""
# =====================================================================
# === imports
# Python imports should always be at the top of the file.
# You probably don't need to worry about these much and will
# just copy-paste them into your DAG.
# =====================================================================
# python std librarys
from datetime import datetime

# non-std python librarys
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

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
AREA_ID = 1
AREA_SHORT_NAME = "gom"
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
DAG_NAME = "example"
DAG_ID = get_dag_id(
    __file__, region=AREA_SHORT_NAME, dag_name=DAG_NAME
)
this_dag = DAG(
    dag_id=DAG_ID,
    # KEEP ONLY ONE SECTION BELOW:
    # === 1) for granule-datematched coverage checks
    default_args=get_default_args(
        # this start date should be the first granule to ingest
        start_date=datetime(2001, 11, 28, 14, 00, 00)
    ),
    schedule_interval=timedelta(minutes=5),  # the frequency of granules
    catchup=True,  # latest only
    # ===
    # === 2) for latest-only coverage checks:
    schedule_interval=timedelta(minutes=5),  # how often to check
    catchup=False,  # latest only
    max_active_runs=1,
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
run_example_ingest_script = BashOperator(
    task_id='run_example_ingest_script',

    bash_command="",
    params={
        "region_name": AREA_SHORT_NAME,
        "area_id": AREA_ID,
    },
    dag=this_dag,
)

# That's it!
# If there were multiple operators defined above you might want to connect
# them up like :
#     run_example_ingest_script >> other_operator
# but since we have only the one we are done with this DAG.
