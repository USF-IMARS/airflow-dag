"""
This is an example IMaRS processing DAG.
It is heavily commented and demonstrates a very basic usage.

This first section is called a docstring and should summarize the contents of the file.
"""
# =====================================================================
# === imports
# Python imports should always be at the top of the file.
# You probably don't need to worry about these much and will
# just copy-paste them into your DAG.
# =====================================================================
# std libs
from datetime import datetime
import os

# deps
from airflow import DAG

# this package
from imars_dags.util.get_dag_id import get_dag_id
from imars_dags.util.get_default_args import get_default_args
from imars_dags.operators.IMaRSETLBashOperator import IMaRSETLBashOperator
from imars_dags.util.globals import QUEUE
# =====================================================================
# =====================================================================
# === Product ID constants
# These constants come from the imars_product_metadata database.
# The product ID number uniquely identifies each data product.
# 
# You may find (and add) product IDs to the SQL database using this file:
# https://github.com/USF-IMARS/imars_puppet/blob/test/modules/role/files/sql/product_metadata_rows.sql
# =====================================================================
# | 36 | s3a_ol_1_efr             |
CP_INPUT_PRODUCT_ID = 123
CP_OUTPUT_PRODUCT_ID = 456  # TODO
L3_PRODUCT_ID = 99  # TODO
# =====================================================================
# =====================================================================
# === Area ID constants
# Like the product IDs, these come from the imars_product_metadata
# database. 
# The region ID and name must match values in the sql file:
# https://github.com/USF-IMARS/imars_puppet/blob/test/modules/role/files/sql/product_metadata_rows.sql
# =====================================================================
AREA_SHORT_NAME = "gom"
AREA_ID = 1
# =====================================================================
# =====================================================================
# === Initial DAG Setup
# Besides changing DAG_NAME, you probably won't need to change anything
# else here.
# =====================================================================
# DAG_NAME should be same as the filename (without .py extention).
DAG_NAME = "example"
DAG_ID = get_dag_id(
    __file__, region=AREA_SHORT_NAME, dag_name=DAG_NAME
)
this_dag = DAG(
    dag_id=DAG_ID,
    default_args=get_default_args(
        start_date=datetime.utcnow()
    ),
    schedule_interval=None,
)
# =====================================================================
# =====================================================================
# === Defining the first operator.
# Operators are how you define a type of "job".
# Here we create an IMaRSETLBashOperator because we want to run a 
# bash command.
# There are no input or output files, we just print an output.
# This is probably way simpler than anything you will ever actually
# create.
# =====================================================================
# the variable name for the operator (hello_world here) should match
# the "task_id".
hello_world = IMaRSETLBashOperator(
    task_id='hello_world',
    # The bash_command here prints "Hello gom!" using the echo command.
    #
    # *Template Variables* inside of {{double braces}} are used to tell
    # airflow where to inject data.
    # In this case we inject "gom" (from AREA_SHORT_NAME above) by
    # passing it in "params" below.
    # Rather than passing the full bash_command as a string like we do
    # here, bash_command will also work with a filename to a .sh file.
    bash_command="echo Hello {{params.region_name}}!",
    inputs={
        # no input files for this job
    },
    outputs={
        # no output files for this job
    },
    },
    tmpdirs=[
        # no temporary directories needed for this
    ],
    params={
        "region_name": AREA_SHORT_NAME,
    },
    # Queues are used to require certain software packages. 
    # They are not yet documented, so ask Tylar or don't use a queue at all
    # by removing this line.
    queue=QUEUE.SAT_SCRIPTS,
    dag=this_dag,
)
# =====================================================================
# =====================================================================
# === An Operator With Input & Ouput
# This slightly more realistic example takes one file as input
# and outputs one file.
# The "job" itself is just to copy the file; your processing would 
# probably do more to transform the data, but the principle is the same.
# =====================================================================
copy_file = IMaRSETLBashOperator(
    task_id="copy_file",
    # We use the program `cp` in verbose mode (`-v`) to copy from 
    # the input file to the output file.
    bash_command="""
        cp -v \
        {{ params.my_input_file }} \
        {{ params.my_output_file }}
    """,
    params={

    },
    inputs={
        # the "name" of the input here can be anything, but note 
        # how it is used in bash_command above.
        "my_input_file":
            # This input_file "selector string" is a MySQL to find your file
            # in the imars_product_metadata db.
            # If you set CP_INPUT_PRODUCT_ID above, and this DAG is
            # "Granule Date-matched" (ie airflow execution_date == observation granule start date),
            # then you do not need to change this.
            "product_id="+str(CP_INPUT_PRODUCT_ID)+" AND date_time='{{ts}}'"
    },
    outputs={
        # Like in "inputs", you can change the name here so long as it matches the bash_command.
        # The only parts of this you need to worry about are the CONSTANTS which were set above.
        # ...Unless your DAG is not Granule-Date-Matched - in that case you need to set the 
        # various times in some other way.
        'my_output_file': {
            "product_id": CP_OUTPUT_PRODUCT_ID,
            # json, sql, & time set up metadata for the imars_product_metadata db 
            # so that this outputs can be found by other scripts.
            "json": '{'
                '"status_id":3,'  # noqa E131
                '"area_short_name":"' + AREA_SHORT_NAME + '"'
            '}',
            "sql": (
                "product_id={} AND area_id={} ".format(
                        L3_PRODUCT_ID, AREA_ID
                ) +
                " AND date_time='{{ execution_date }}'" 
            ),
            "time": "{{ ts }}",
            # These modify details of how products are loaded. For Basic usage just copy-paste
            # or remove these lines.
            'duplicates_ok': False, 
            'nohash': False,
            "verbose": 3,
        },
    },
    dag=this_dag,
)
# =====================================================================
# === wire together jobs
# Here we make sure that the hello_world job runs before the copy_file
# job for each DagRun. 
# There is no reason to do this in this example, but for demo purposes
# pretend that hello_world produces some output that copy_file needs.
# =====================================================================

hello_world >> copy_file
# =====================================================================
