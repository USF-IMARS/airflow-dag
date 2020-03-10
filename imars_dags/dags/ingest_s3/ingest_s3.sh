#!/bin/bash
# Script for daily downloading Sentinel 3 files within coverage area.
#
# NOTE: passwd is in .netrc file

# these dates come from airflow. example: 20191130
START_DATE="{{prev_ds_nodash}}"
END_DATE="{{ds_nodash}}"

mkdir s3files

sentinelsat \
	-d \
	--url "https://scihub.copernicus.eu/dhus" \
	-g "florida.geojson" \
	-s $START_DATE -e $END_DATE \
	--producttype "OL_1_EFR___" \
	--path "s3files"
	# -- sentinel 3
	# -- instrument ???
	# -- cloud 30

find ./s3files -name 'S3A_OL_1_EFR__*.zip' \
	| xargs -n 1 imars-etl load \
		--duplicates_ok \
		--sql 'product_id=12 AND area_id=9 AND provenance="s3_api_af_v01"' \
		--load_format 'S3{sat_id}_OL_1_EFR____%Y%m%dT%H%M%S_{end_date:08d}T{end_t:06d}_{ing_date:08d}T{ing_t:06d}_{duration:04d}_{cycle:03d}_{orbit:03d}_{frame:04d}_{proc_location}_{platform}_{timeliness}_{base_collection:03d}.zip'
