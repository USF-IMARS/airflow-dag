export OCSSWROOT=/opt/ocssw      &&
source /opt/ocssw/OCSSW_bash.env &&
OUT_PATH={{ GEOFILE|render }}             &&
$OCSSWROOT/scripts/modis_GEO.py \
    --output=$OUT_PATH \
    {{ MYD01FILE|render }} &&
[[ -s $OUT_PATH ]]
