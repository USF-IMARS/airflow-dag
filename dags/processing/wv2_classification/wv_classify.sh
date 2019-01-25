# wv2 habitat classification adapted from
# https://github.com/USF-IMARS/wv2-processing/blob/master/submit_py.sh
BASENAME=wv02_19890607101112_fake0catalog0id0
PGC_SUFFIX=_u16ns4326
MET_BASENAME=$BASENAME.xml
ORTH_BASENAME=${BASENAME}$PGC_SUFFIX.tif

INPUT_DIR=input
ORTHO_DIR=ortho
RRS_OUT=output
CLASS_OUT=$RRS_OUT

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
cd input_dir
# NOTE: I think these two must have the same file basename:
# === ntf image is product_id # 11
imars-etl extract "product_id=11 AND date_time='{{ts}}'"
# TODO: add area here? "area_id={{params.area_id}}"
ORTH_BASENAME=???  # TODO: get filename
# === met xml is product_id # 14
imars-etl extract "product_id=14 AND date_time='{{ts}}'"
# TODO: add area here? "area_id={{params.area_id}}"
MET_BASENAME=???  # TODO: get filename?
# TODO: get (ensure?) common basename
BASENAME=???
cd ..

# =================================================================
# === TRANSFORM
# =================================================================
# === pgc ortho
ORTH_FILE=$ORTH_DIR/$ORTHO_BASENAME
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
MET2=$ORTH_DIR/${BASENAME}${PGC_SUFFIX}.xml

# TODO: use python instead:
# === matlab
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
        '$RRS_OUT',\
        '$CLASS_OUT'\
    );\
    exit\
" &&
[[ -s $RRS_OUT/{{params.id}}_{{params.loc}}_Rrs.tif ]]  # Rrs should always be output

# =================================================================
# === LOAD
# =================================================================
# TODO:



# # figure out what filenames should be:
# # ===========================================================================
# if FILTER:
#     classf_output = "output_dir/{}_{}_DT_filt_{}_{}_{}.tif".format(
#         ID, LOC, ID_NUM, FILTER, STAT
#     )
# else:
#     classf_output = "output_dir/{}_{}_DT_nofilt_{}.tif".format(
#         ID, LOC, ID_NUM
#     )
# # expected output filepaths
# Rrs_output = "output_dir/{}_{}_Rrs.tif".format(ID, LOC)
# rrs_output = "output_dir/{}_{}_rrs.tif".format(ID, LOC)
# bth_output = "output_dir/{}_{}_Bathy.tif".format(ID, LOC)
# outputs_to_load = {
#     Rrs_output: {  # Rrs always an output
#         "noparse": True,
#         "time": "{{ ts }}",
#         "json": JSON,
#         "sql": 'area_id={} AND product_id={}'.format(area_id, Rrs_ID),
#         "nohash": True,  # TODO: rm one reprocess is complete
#     }
# }
#
# if DT == 0:
#     # Rrs only
#     pass
# elif DT in [1, 2]:
#     # Rrs, rrs, bathymetry for DT in [1,2]
#     outputs_to_load[rrs_output] = {
#         "noparse": True,
#         "time": "{{ ts }}",
#         "json": JSON,
#         "sql": 'area_id={} AND product_id={}'.format(area_id, rrs_ID)
#     }
#     outputs_to_load[bth_output] = {
#         "noparse": True,
#         "time": "{{ ts }}",
#         "json": JSON,
#         "sql": 'area_id={} AND product_id={}'.format(area_id, bth_ID)
#     }
# else:
#     raise ValueError("DT must be 0,1,2")
#
# if DT == 1:
#     # Rrs, rrs, bathymetry **and classification**
#     outputs_to_load[classf_output] = {
#         "noparse": True,
#         "time": "{{ ts }}",
#         "json": JSON,
#         "sql": 'area_id={} AND product_id={}'.format(area_id, classf_ID)
#     }
