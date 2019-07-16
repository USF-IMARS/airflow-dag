#!/bin/bash
# The line above is called a "shebang".
# It is required. For bash scripts you can copy that one.

# =======================================================================
# This is an example ingest bash script which accompanies
# "./doc/ingest_dags/example.py".
# You should probably read that file first.
#
# This script downloads a file from the internet and then loads it into
# IMaRS's systems so that it will be stored safely and made usable for
# processing.
# =======================================================================
# =======================================================================
# The bash_command here prints "Hello world!" using the echo command.
echo Hello world!
# =======================================================================
# =======================================================================
# Variables from the DAG (example.py) can be used here.
# Let's `echo` the name of our region from `example.py`.
#
# We print "Checking gom RoI..." using *Template Variables* inside
# of {{double braces}} to tell airflow where to inject data.
#
# In this case we inject "gom" (from AREA_SHORT_NAME) by
# including `"region_name": AREA_SHORT_NAME` in the "params"
# *keyword argument* of the `run_example_ingest_script` `BashOperator`.
echo Checking {{params.region_name}} RoI...
# =======================================================================
# =======================================================================
# Save a webpage to file as an easy example.
curl http://www.google.com > file_i_want_to_ingest.html
# =======================================================================
# =======================================================================
# Now we can load the file to complete our ingest job.
# In this example metadata is passed using SQL.
# `imars-etl` can guess many things, but at a minimum you should pass
# area_id and provenance here.
# Read more from `imars-etl load --help` or
# [imars-etl](https://github.com/USF-IMARS/imars-etl) documentation.
imars-etl load \
    --sql \
        "area_id={{params.area_id}} AND provenance='example_ingest_script_v1'" \
    file_i_want_to_ingest.html
# =======================================================================
