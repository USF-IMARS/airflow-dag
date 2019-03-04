#!/bin/bash
set -e

echo '=== Extract...'
# TODO: use this:
# L1_PATH=$( \
#     imars-etl extract \
#         'product_id={{params.l1_pid}} AND date_time="{{ts}}"'
# )
# instead of this:
imars-etl extract \
    'product_id={{params.l1_pid}} AND date_time="{{ts}}"'
L1_PATH=$(ls *.SEN3)  # should this be *.zip?


echo '=== Transform...'

# set environment variables
OCSSWROOT=/opt/ocssw
export OCSSWROOT=/opt/ocssw

set +e
source $OCSSWROOT/OCSSW_bash.env
set -e

echo unzipping...
mv $L1_PATH $L1_PATH.zip
unzip $L1_PATH.zip -d $L1_PATH

# find the file we're looking for inside the unzipped dir
L1_SEN3_XML=$(find $L1_PATH/ -name xfdumanifest.xml -type f -print0)
# NOTE: what if we find more than one?

echo running l2gen...
L2_PATH='l2_file.nc'
l2gen ifile=${L1_SEN3_XML} ofile=$L2_PATH par='{{ params.par }}'


echo '=== Load...'
imars-etl load \
    --sql 'product_id={{params.l2_pid}} AND area_id={{params.area_id}} AND date_time="{{execution_date}}" AND provenance=" AND provenance="af-l1tol2_v1""'\
    --json '{"area_short_name":"{{params.area_short_name}}"}'\
    $L2_PATH
