#!/bin/bash

L1_PATH={{ params.s3_file }}
L2_PATH={{ params.l2_file }}
PARFILE_PATH={{ params.par }}

# set environment variabless
export OCSSWROOT=/opt/ocssw
source $OCSSWROOT/OCSSW_bash.env

echo running l2gen...
l2gen ifile=$L1_PATH ofile=$L2_PATH par=$PARFILE_PATH
