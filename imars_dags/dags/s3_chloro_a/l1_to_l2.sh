#!/bin/bash
L2_SQL="product_id={{params.l2_pid}} AND area_id={{params.area_id}} AND date_time=\"{{execution_date}}\" AND provenance=\"af-l1tol2_v1\""
if imars-etl select "$L2_SQL" ;
then
    echo "L2 product already exists"
else
    set -e
    echo '=== Extract...'
    # TODO: use this:
    # L1_PATH=$( \
    #     imars-etl extract \
    #         'product_id={{params.l1_pid}} AND date_time="{{ts}}"'
    # )
    # instead of this:
    imars-etl -vvv extract \
        'product_id={{params.l1_pid}} AND date_time="{{ts}}"'
    L1_PATH=$(ls *.zip)


    echo '=== Transform...'

    # set environment variables
    OCSSWROOT=/opt/ocssw
    export OCSSWROOT=/opt/ocssw

    set +e
    source $OCSSWROOT/OCSSW_bash.env
    set -e

    # remove .zip extension
    L1_PATH=$(basename $L1_PATH .zip)

    unzip $L1_PATH.zip -d $L1_PATH

    # find the file we're looking for inside the unzipped dir
    L1_SEN3_XML=$(find $L1_PATH/ -name xfdumanifest.xml -type f -print0)
    # NOTE: what if we find more than one?

    echo running l2gen...
    L2_PATH='l2_file.nc'
    l2gen ifile=${L1_SEN3_XML} ofile=$L2_PATH par='{{ params.par }}'


    echo '=== Load...'
    imars-etl -vvv load --duplicates_ok \
        --sql "$L2_SQL" \
        $L2_PATH
fi
