#!/bin/bash
# set environment variabless
# TODO: OCSSWROOT stuff not needed here?
export OCSSWROOT=/opt/ocssw
source $OCSSWROOT/OCSSW_bash.env
set -e

echo '=== Extract...'
L2_PATH='input.nc'
imars-etl extract -o $L2_PATH \
    "product_id={{params.input_pid}} AND area_id={{params.input_area_id}} AND date_time='{{ts}}'"

GPT_XML={{ params.gpt_xml }}
OUTFILE='mapped.tif'

echo '=== Transform...'
echo mapping w/ ${GPT_XML}...
/opt/snap_6_0/bin/gpt $GPT_XML -t $OUTFILE -f GeoTIFF $L2_PATH

echo "output file $OUTFILE is ready?"
ls -lh .
# TODO: assert $OUTFILE exists & size is reasonable
echo "I guess so?"

echo '=== Load...'
imars-etl load --sql \
    "product_id={{params.p_id}} AND area_id={{params.area_id}} AND date_time='{{ts}}'" \
    $OUTFILE
