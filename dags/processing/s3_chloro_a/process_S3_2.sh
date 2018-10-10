#!/bin/bash

# processes given image filename
# usage:	./process_images.sh [filename]
# example:	./process_images.sh S3A_OL_1_EFR____2017011.SEN3 map_OKA_S3_OLCI.xml
L1_PATH={{ params.s3_file }}
L2_PATH={{ params.l2_file }}
PARFILE_PATH={{ params.par }}
CHAR_xml={{ params.xml_filec }}
PIN_xml={{ params.xml_filep }}
OKA_xml={{ params.xml_fileo }}
FLY_xml={{ params.xml_filef }}


# give parameters useful names
FILENAME=$1  # eg S3A_OL_1_EFR____20170118.SEN3


# set environment variabless
export OCSSWROOT=/opt/ocssw
source $OCSSWROOT/OCSSW_bash.env

echo running l2gen...
l2gen ifile=$L1_PATH ofile=$L2_PATH par=$PARFILE_PATH

'''
echo running mapping graph...
/opt/snap_6_0/bin/gpt $CHAR_xml -t $DATA_DIR/S3_map/$FILENAME.C.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2

echo running mapping graph...
/opt/snap_6_0/bin/gpt $PIN_xml  -t $DATA_DIR/S3_map/$FILENAME.P.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2

echo running mapping graph...
/opt/snap_6_0/bin/gpt $OKA_xml  -t $DATA_DIR/S3_map/$FILENAME.O.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2

echo running mapping graph...
/opt/snap_6_0/bin/gpt $FLY_xml  -t $DATA_DIR/S3_map/$FILENAME.F.map -f GeoTIFF $DATA_DIR/S3_proc/$FILENAME.L2
'''
