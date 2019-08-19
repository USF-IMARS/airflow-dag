# wv2 habitat classification adapted from
# https://github.com/USF-IMARS/wv2-processing/blob/master/submit_py.sh
INPUT_DIR=input
ORTHO_DIR=ortho
RRS_OUT=output
CLASS_OUT=$RRS_OUT

rrs_SQL="area_id={{params.area_id}} AND product_id={{params.Rrs_ID}} AND date_time='{{ts}}' AND provenance='af-ntftorrs_v2'"
RRS_SQL="area_id={{params.area_id}} AND product_id={{params.rrs_ID}} AND date_time='{{ts}}' AND provenance='af-ntftorrs_v2'"
MAP_SQL="area_id={{params.area_id}} AND product_id={{params.classf_ID}} AND date_time='{{ts}}' AND provenance='af-ntftorrs_v2'"

if imars-etl select $rrs_SQL && imars-etl select $RRS_SQL && imars-etl select $MAP_SQL ;
then
    echo "rrs, RRS, and classification map products already exist."
else
    set -e

    mkdir $INPUT_DIR
    mkdir $ORTHO_DIR
    mkdir $RRS_OUT
    # mkdir $CLASS_OUT  # not needed b/c same as RRS_OUT

    # =================================================================
    # === EXTRACT
    # =================================================================
    # NOTE: basename *must* have subsring that matches one of the regexes in
    # [imagery_utils.lib.utils.get_sensor](https://github.com/PolarGeospatialCenter/imagery_utils/blob/v1.5.1/lib/utils.py#L57)
    # (if not we get USF-IMARS/imars_dags#64) so here we match
    # "(?P<ts>\d\d[a-z]{3}\d{8})-(?P<prod>\w{4})?(?P<tile>\w+)?-(?P<oid>\d{12}_\d\d)_(?P<pnum>p\d{3})"
    # using a hard-coded sensor "wv02" + a fake date & catalog id
    cd $INPUT_DIR
    # NOTE: I think these two must have the same file basename:
    # === ntf image is product_id # 11
    imars-etl extract "product_id=11 AND date_time='{{ts}}' AND area_id='{{params.area_id}}'"
    ORIG_NTF_BASENAME=$(ls *.ntf)  # TODO: this is dumb

    # === met xml is product_id # 14
    imars-etl extract "product_id=14 AND date_time='{{ts}}' AND area_id='{{params.area_id}}'"
    ORIG_MET_BASENAME=$(ls *.xml)  # TODO: also dumb

    # === rename files so we can predict what pgc_ortho will do.
    BASENAME=wv02_19890607101112_fake0catalog0id0

    MET_BASENAME=$BASENAME.xml
    mv $ORIG_MET_BASENAME $MET_BASENAME

    NTF_BASENAME=$BASENAME.ntf
    mv $ORIG_NTF_BASENAME $NTF_BASENAME

    PGC_SUFFIX=_u16ns4326
    ORTHO_BASENAME=${BASENAME}$PGC_SUFFIX.tif

    cd ..

    # =================================================================
    # === TRANSFORM
    # =================================================================
    # === pgc ortho
    ORTHO_FILE=$ORTHO_DIR/$ORTHO_BASENAME
    python /opt/imagery_utils/pgc_ortho.py \
        -p 4326 \
        -c ns \
        -t UInt16 \
        -f GTiff \
        --no-pyramids \
        $INPUT_DIR \
        $ORTHO_DIR &&
        [[ -s $ORTHO_FILE ]]

    MET=$INPUT_DIR/$MET_BASENAME
    # pgc moves the xml file from $MET to $MET2 and modifies it
    MET2=$ORTHO_DIR/${BASENAME}${PGC_SUFFIX}.xml

    python3.6 -m wv_classify.wv_classify \
        $ORTHO_FILE $MET2 $RRS_OUT FAKELOC "EPSG:4326" 2 1

    # =================================================================
    # === LOAD
    # =================================================================
    RRS_TIF_PATH=$(ls ${RRS_OUT}/*Rrs.tif)  # TODO: also dumb
    rrs_TIF_PATH=$(ls ${RRS_OUT}/*_rrssub.tif)  # TODO: also also dumb
    MAP_TIF_PATH=$(ls ${RRS_OUT}/*_Map_pytest.tif)  # TODO: also also also dumb

    imars-etl load --noparse --duplicates_ok --sql \
        $RRS_SQL \
        $RRS_TIF_PATH

    imars-etl load --noparse --duplicates_ok --sql \
        $rrs_SQL \
        $rrs_TIF_PATH

    imars-etl load --noparse --duplicates_ok --sql \
        $MAP_SQL \
        $MAP_TIF_PATH

    # =================================================================
fi
