# wv2 habitat classification adapted from
# https://github.com/USF-IMARS/wv2-processing/blob/master/submit_py.sh
BASENAME=wv02_19890607101112_fake0catalog0id0
PGC_SUFFIX=_u16ns4326
MET_BASENAME=$BASENAME.xml
ORTH_BASENAME=${BASENAME}$PGC_SUFFIX.tif

INPUT_DIR={{params.input_dir}}
ORTH_DIR={{ params.ortho_dir }}
RRS_OUT={{ params.output_dir }}
CLASS_OUT={{ params.output_dir }}

ORTH_FILE=$ORTH_DIR/$ORTH_BASENAME
MET=$INPUT_DIR/$MET_BASENAME

# === pgc ortho
python /opt/imagery_utils/pgc_ortho.py \
    -p 4326 \
    -c ns \
    -t UInt16 \
    -f GTiff \
    --no-pyramids \
    $INPUT_DIR \
    $ORTH_DIR &&
    [[ -s $ORTH_FILE ]]

# pgc moves the xml file from $MET to $MET2 and modifies it
MET2=$ORTH_DIR/${BASENAME}$PGC_SUFFIX.xml
# === matlab
/opt/matlab/R2018a/bin/matlab -nodisplay -nodesktop -r "\
    cd('/opt/wv2_processing');\
    wv2_processing(\
        \"$ORTH_FILE\",\
        '{{params.id}}',\
        \"$MET2\",\
        '{{params.crd_sys}}',\
        '{{params.dt}}',\
        '{{params.sgw}}',\
        '{{params.filt}}',\
        '{{params.stat}}',\
        '{{params.loc}}',\
        '{{params.id_number}}',\
        \"$RRS_OUT\",\
        \"$CLASS_OUT\"\
    );\
    exit\
" &&
[[ -s $RRS_OUT/{{params.id}}_{{params.loc}}_Rrs.tif ]]  # Rrs should always be output
