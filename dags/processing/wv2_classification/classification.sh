# wv2 habitat classification adapted from
# https://github.com/USF-IMARS/wv2-processing/blob/master/submit_py.sh

ORTH_FILE={{ params.ortho_dir }}/wv02_19890607101112_fake0catalog0id0_u16ns4326.tif &&

# === pgc ortho
python /opt/imagery_utils/pgc_ortho.py \
    -p 4326 \
    -c ns \
    -t UInt16 \
    -f GTiff \
    --no-pyramids \
    {{ params.input_dir }} {{ params.ortho_dir }} &&
    [[ -s $ORTH_FILE ]]

# === matlab
MET={{params.input_dir}}/wv02_19890607101112_fake0catalog0id0.xml  &&
/opt/matlab/R2018a/bin/matlab -nodisplay -nodesktop -r "\
    cd('/opt/wv2_processing');\
    wv2_processing(\
        '$ORTH_FILE',\
        '{{params.id}}',\
        '$MET',\
        '{{params.crd_sys}}',\
        '{{params.dt}}',\
        '{{params.sgw}}',\
        '{{params.filt}}',\
        '{{params.stat}}',\
        '{{params.loc}}',\
        '{{params.id_number}}',\
        '""" + rrs_out + """',\
        '""" + class_out + """'\
    );\
    exit\
" &&
[[ -s """ + Rrs_output + """ ]]""",  # Rrs should always be output
