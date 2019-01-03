#!/bin/bash

L1_PATH={{ params.s3_file }}
L2_PATH={{ params.l2_file }}
PARFILE_PATH={{ params.par }}

# set environment variabless
export OCSSWROOT=/opt/ocssw
source $OCSSWROOT/OCSSW_bash.env

echo unzipping...
mv $L1_PATH $L1_PATH.zip
unzip $L1_PATH.zip -d $L1_PATH

# find the file we're looking for inside the unzipped dir
L1_SEN3_DIR=$(find $L1_PATH/ -maxdepth 1 -name *SEN3 -type d -print)

echo running l2gen...
l2gen ifile=$L1_SEN3_DIR/xfdumanifest.xml ofile=$L2_PATH par=$PARFILE_PATH
