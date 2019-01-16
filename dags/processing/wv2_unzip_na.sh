set -e

echo extract...
# TODO: use this:
# WV2_ZIP_FILE=$(
#     imars-etl extract 'product_id=6 AND date_time="{{ts}}"'
# )
# instead of this:
imars-etl extract 'product_id=6 AND date_time="{{ts}}"'
WV2_ZIP_FILE=$(ls *.zip)

echo transform...
UNZIPPED_DIRNAME=wv2_unzipped
mkdir $UNZIPPED_DIRNAME

unzip $WV2_ZIP_FILE -d $UNZIPPED_DIRNAME
# NOTE: GIS_FILES need to be removed so they aren't accidentally ingested.
rm -r $UNZIPPED_DIRNAME/*/*/GIS_FILES

echo load...
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.ATT | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=7 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.ATT | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=8 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.GEO | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=9 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.GEO | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=22 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.IMD | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=10 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.IMD | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=23 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
find $UNZIPPED_DIRNAME -name *M1BS* -name *.NTF | \
    xargs -n 1 \
    imars-etl load \
    --sql 'product_id=11 AND area_id={{params.area_id}}' \
    --metadata_file_driver wv2_xml \
    --metadata_file '{directory}/{basename}.XML' \
    --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.NTF | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=24 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.RPB | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=12 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.RPB | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=25 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.TIL | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=13 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML'
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.TIL | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=26 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
find $UNZIPPED_DIRNAME -name *M1BS* -name *.XML | \
    xargs -n 1 \
    imars-etl load \
    --sql 'product_id=14 AND area_id={{params.area_id}}' \
    --metadata_file_driver wv2_xml \
    --metadata_file '{directory}/{basename}.XML' \
    --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.XML | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=27 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# NOTE: skip the -BROWSE.JPG files b/c of filename.xml issues
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.JPG | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=15 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.JPG | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=28 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# NOTE: skip README.TXT files & README.xml b/c meh
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.SHX | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=17 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.SHX | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=30 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.SHP | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=18 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.SHP | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=31 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.DBF | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=19 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.DBF | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=32 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *M1BS* -name *.EPH | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=33 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
# find $UNZIPPED_DIRNAME -name *P1BS* -name *.EPH | \
#     xargs -n 1 \
#     imars-etl load \
#     --sql 'product_id=21 AND area_id={{params.area_id}}' \
#     --metadata_file_driver wv2_xml \
#     --metadata_file '{directory}/{basename}.XML' \
#     --duplicates_ok
