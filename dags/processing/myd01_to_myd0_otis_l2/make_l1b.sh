export OCSSWROOT=/opt/ocssw &&
source /opt/ocssw/OCSSW_bash.env &&
OKM_PATH={{ OKMFILE|render }} &&
HKM_PATH={{ HKMFILE|render }} &&
QKM_PATH={{ QKMFILE|render }} &&
$OCSSWROOT/scripts/modis_L1B.py \
    --okm $OKM_PATH \
    --hkm $HKM_PATH \
    --qkm $QKM_PATH \
    {{ MYD01FILE|render }} \
    {{ GEOFILE|render }} &&
[[ -s $OKM_PATH ]]
# NOTE: might want to add `&& -s $HKM_PATH && -s $QKM_PATH` too
