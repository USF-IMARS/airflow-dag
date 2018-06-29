
export OCSSWROOT=/opt/ocssw &&
source /opt/ocssw/OCSSW_bash.env &&
$OCSSWROOT/bin/l2gen \
    ifile={{ OKMFILE|render }} \
    ofile={{ L2FILE|render }} \
    geofile={{ GEOFILE|render }} \
    xcalfile={{ params.xcalfile }} \
    par={{ params.parfile }} &&
[[ -s {{ L2FILE|render }} ]]
