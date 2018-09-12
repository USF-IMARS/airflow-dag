#!/bin/bash

# processes given image filename
# usage:	./process_images.sh [filename]
# example:	./process_images.sh S3A_OL_1_EFR____2017011.SEN3 map_OKA_S3_OLCI.xml

# give parameters useful names
FILENAME=$1  # eg S3A_OL_1_EFR____20170118.SEN3
XMLFILE=$2   # eg map_OKA_S3_OLCI.xml

# set environment variabless
export OCSSWROOT=/opt/ocssw
source $OCSSWROOT/OCSSW_bash.env

DATA_DIR=/home1/sebastian/s3proc


echo running mapping graph...
/opt/snap_6_0/bin/gpt $XMLFILE -t $DATA_DIR/S3_map/$FILENAME.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2


