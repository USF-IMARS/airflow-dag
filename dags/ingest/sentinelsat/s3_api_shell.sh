#!/bin/bash

echo '== Extact..'
florida_map_json=$( \
    imars-etl extract \
    'florida_geojson={{params.florida_geojson}}'
)
s3_metadata_json = $( \
    imars-etl extract \
    'meta_s3={{params.metadata_s3}}'
)

s3_api = $( \
    imars-etl extract \
    's3-api={{params.s3_api_python}}'
)

./s3_api florida_map_json s3_metadata_json

s3_api.py --help
