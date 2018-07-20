#!/bin/bash
# Modis Aqua l1 to l2 processing.
# Creates then cleans up temporary geo & hkm files.
# Based on `process_image.sh` from IMaRS's `scratch/epa/satellite/modis/GOM`.
# Contacts: Tylar Murray, Dan Otis
TMP_DIR={{ params.tmp_dir }}
L1_PATH={{ params.myd01_file }}
L2_PATH={{ params.l2_file }}
XCALFILE_PATH={{ params.xcalfile }}
PARFILE_PATH={{ params.par }}
GEO_PATH=$TMP_DIR/geo
OKM_PATH=$TMP_DIR/okm
HKM_PATH=$TMP_DIR/hkm
QKM_PATH=$TMP_DIR/qkm

# set environment variables
export OCSSWROOT=/opt/ocssw
source $OCSSWROOT/OCSSW_bash.env

# script should exit immedidately if any command fails
# to change this comment out the following line or change to `set +e`
set -e

# echo unzipping...
# bzip2 -d $TMP_DIR/l1a_A/$FILENAME.L1A_LAC.x.hdf.bz2

echo running modis_GEO...
$OCSSWROOT/scripts/modis_GEO.py \
    --output=$GEO_PATH \
    $L1_PATH &&
[[ -s $L1_PATH ]]


echo running modis_L1B...
$OCSSWROOT/scripts/modis_L1B.py \
    --okm $OKM_PATH \
    --hkm $HKM_PATH \
    --qkm $QKM_PATH \
    $L1_PATH \
    $GEO_PATH &&
[[ -s $OKM_PATH ]]
# NOTE: might want to add `&& -s $HKM_PATH && -s $QKM_PATH` too

echo running l2gen...
# NOTE: xcalfile must be set for v7.4 and will need to be updated ~ 1/mo
#     for more info see:
#     https://oceancolor.gsfc.nasa.gov/forum/oceancolor/topic_show.pl?pid=37506
# TODO: (I think) this can be removed now that all nodes are v7.5+
$OCSSWROOT/bin/l2gen \
    ifile=$OKM_PATH \
    ofile=$L2_PATH \
    geofile=$GEO_PATH \
    xcalfile=$XCALFILE_PATH \
    par=$PARFILE_PATH &&
[[ -s $L2_PATH ]]

# NOTE: IMaRSETLBashOperator takes care of this
# echo cleaning up tmp files...
# rm $GEOPATH
# rm $OKM_PATH
# rm $HKM_PATH
# rm $QKM_PATH
# rm $TMP_DIR
