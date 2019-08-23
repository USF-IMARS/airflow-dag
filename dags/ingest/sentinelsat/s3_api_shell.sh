#!/bin/bash

set -e

echo '== Extact..'
florida_map_json={{params.florida_geojson}}
s3_metadata_json={{params.metadata_s3}}
s3_api={{params.s3_api_python}}
s3_meta_append={{params.metadata_s3_appended}}
s3_date={{ds}}

#python3 $s3_api --help

python3 $s3_api --geojson $florida_map_json --meta $s3_metadata_json \
    --append $s3_meta_append --date $s3_date
