#!/bin/bash

set -e

echo '== Extact..'
florida_map_json={{params.florida_geojson}}
s3_metadata_json={{params.metadata_s3}}
s3_api={{params.s3_api_python}}

# $s3_api --help

python3 $s3_api $florida_map_json $s3_metadata_json
