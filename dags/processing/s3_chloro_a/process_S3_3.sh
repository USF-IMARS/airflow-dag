#!/bin/bash

# processes given image filename
# usage:	./process_images.sh [filename]
# example:	./process_images.sh S3A_OL_1_EFR____2017011.SEN3 map_OKA_S3_OLCI.xml

L2_PATH={{ params.l2_file }}

CHAR_xml={{ params.xml_filec }}
PIN_xml={{ params.xml_filep }}
OKA_xml={{ params.xml_fileo }}
FLY_xml={{ params.xml_filef }}

C_MAP={{ params.c_map }}
P_MAP={{ params.p_map }}
O_MAP={{ params.o_map }}
F_MAP={{ params.f_map }}

# set environment variabless
export OCSSWROOT=/opt/ocssw
source $OCSSWROOT/OCSSW_bash.env

echo mapping Charlotte Bay...
/opt/snap_6_0/bin/gpt $CHAR_xml -t $C_MAP -f GeoTIFF $L2_PATH

echo mapping Pinellas...  # I think...
/opt/snap_6_0/bin/gpt $PIN_xml  -t $P_MAP -f GeoTIFF $L2_PATH

echo mapping Oka-something...
/opt/snap_6_0/bin/gpt $OKA_xml  -t $O_MAP -f GeoTIFF $L2_PATH

echo mapping Fly-place...
/opt/snap_6_0/bin/gpt $FLY_xml  -t $F_MAP -f GeoTIFF $L2_PATH
