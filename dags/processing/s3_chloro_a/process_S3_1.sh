#!/bin/bash

# processes given image filename
# usage:	./process_images.sh [filename]
# example:	./process_images.sh S3A_OL_1_EFR____2017011.SEN3 map_OKA_S3_OLCI.xml
#! only needed if adding additional .xml files to an already processed image
# the new .xml file needs to be within the bound of the processed image

# give parameters useful names
FILENAME=$1  # eg S3A_OL_1_EFR____20170118.SEN3
XMLFILE=$2   # eg map_OKA_S3_OLCI.xml

# set environment variabless
export OCSSWROOT=/opt/ocssw
source $OCSSWROOT/OCSSW_bash.env

DATA_DIR=/home1/sebastian/s3proc
# TODO: change to DATA_DIR=/imars_dags/dags/processing/s3_chloro_a 

echo running mapping graph...
/opt/snap_6_0/bin/gpt $XMLFILE -t $DATA_DIR/S3_map/$FILENAME.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2


