#!/bin/bash

# processes given image filename
# usage:	./process_images.sh [filename]
# example:	./process_images.sh S3A_OL_1_EFR____2017011.SEN3 map_OKA_S3_OLCI.xml

# give parameters useful names
FILENAME=$1  # eg S3A_OL_1_EFR____20170118.SEN3


# set environment variabless
export OCSSWROOT=/opt/ocssw
source $OCSSWROOT/OCSSW_bash.env

DATA_DIR=/home1/sebastian/s3proc
# should change to DATA_DIR=/imars_dags/dags/processesing/s3_chloro_a  ??

echo running l2gen...
l2gen ifile=$DATA_DIR/S3_raw/$FILENAME/xfdumanifest.xml ofile=$DATA_DIR/S3_proc/$FILENAME.L2 par=$DATA_DIR/IMaRS_S3_l2gen.par

echo running mapping graph...
/opt/snap_6_0/bin/gpt $DATA_DIR/map_CHAR_S3_OLCI.xml -t $DATA_DIR/S3_map/$FILENAME.C.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2

echo running mapping graph...
/opt/snap_6_0/bin/gpt $DATA_DIR/map_PIN_S3_OLCI.xml -t $DATA_DIR/S3_map/$FILENAME.P.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2

echo running mapping graph...
/opt/snap_6_0/bin/gpt $DATA_DIR/map_OKA_S3_OLCI.xml -t $DATA_DIR/S3_map/$FILENAME.O.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2

echo running mapping graph...
/opt/snap_6_0/bin/gpt $DATA_DIR/map_FLBY_S3_OLCI.xml -t $DATA_DIR/S3_map/$FILENAME.F.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2
