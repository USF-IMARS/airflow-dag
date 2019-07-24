#!/bin/env python
# connect to the API
# https://buildmedia.readthedocs.org/media/pdf/sentinelsat/master/sentinelsat.pdf for help
#can run locally on mobaxterm: source venv/bin/activate
#if first time start with: export SLUGIFY_USES_TEXT_UNIDECODE=yes; virtualenv venv; source venv/bin/activate; pip install -e .[test]; py.test -v
#might have to add: pip install requests-mock; pip install rstcheck; pip install geojson

from datetime import date
import os
import collections
import json
from argparse import ArgumentParser

from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import imars_etl

    
def getJSON_read(filePathandName):
    with open(filePathandName,'r') as infile:
        return json.load(infile)

def getJSON_write(filePathandName,variables):
    with open(filePathandName,'w') as outfile:
        return json.dump(variables,outfile)

#not currently being used, but would block duplicates from being downloaded
#ef remove_dupes(mymetalist):
#    newlist = []
#    for item in mymetalist:
#        data_exist = False
#        for ud in newlist:
#            if ud['properties'] == item['properties']:
#                data_exists = True
#                break
#            else:
#                newlist.append(item)

def main(args):
    # stuff here
    print(args.roi_geojson_fpath)
    print(args.metadata_s3_fpath)
    print(args.s3_meta_append_fpath)

    api = SentinelAPI(None, None, "https://scihub.copernicus.eu/dhus") ##### should we use a general IMARS password and user?
    data_dir = os.getcwd()                                                 # the only way I found to get all the parts of code to work in my directory

    #TODO something to this affect, I looked at dags/processing/s3_chloro_a/l1_to_l2.sh, but not sure since that was a bash
    #and this is python
            #imars-etl extract \
                #'fl_geojson = {{params.florida_geojson}} AND 'metadata' ={{params.metadata_s3}}

    # download single scene by known product id                               #used if downloading one image using the UUID
    #api.download(<product id>)
    #ex api.download('16e7b752-c1a7-4ea0-8107-756005d6c29a')

    # search by polygon, time, and SciHub query keywords                   #where the query starts, GEOJson focuses on florida

    #footprint = geojson_to_wkt(read_geojson(fl_geojson))
    footprint = geojson_to_wkt(read_geojson(args.roi_geojson_fpath))
    products = collections.OrderedDict()
    products = api.query(footprint,
                        date=('20171010', date(2017, 10, 15)),                #two different ways to show date, once we get it going, change the first date to '20150101' and last to 'NOW', then update to be 'NOW-1 and 'NOW'
                        #area_relation({'Intersects','Contains','IsWithin'}) might need to add, default intersect    #propbably wont need
                        platformname='Sentinel-3',
                        producttype='OL_1_EFR___',
                                    )
                                                                           #their are other variable we can add, say if we also want S2 images or another product from S3

    # GeoJSON FeatureCollection containing footprints and metadata of the scenes        #how I get query data to JSON format and into JSON file
    json_query_results = api.to_geojson(products)
    json_stuff = json_query_results['features']

    #adds status : incomplete to the properties in for each image metadata, also deletes useless variable 'id'
    for item in json_stuff:
        item['properties']['status']='Incomplete'
        if 'id' in item:
            del item['id']

    getJSON_write(args.metadata_s3_fpath,json_stuff)
    #makes sure the metadata and appended metadata have data within the files before combining them
    new_meta=[]
    if os.stat(args.metadata_s3_fpath).st_size == 0:
        if os.stat(args.s3_meta_append_fpath).st_size == 0:
            exit()
        else:
            old_meta = getJSON_read(args.s3_meta_append_fpath)
            new_meta.extend(old_meta)
    else:
        metadata = getJSON_read(args.metadata_s3_fpath)
        if os.stat(args.s3_meta_append_fpath).st_size == 0:
            new_meta.extend(metadata)
        else:
            old_meta = getJSON_read(args.s3_meta_append_fpath)
            new_meta.extend(old_meta)
            new_meta.extend(metadata)

    getJSON_write(args.s3_meta_append_fpath,new_meta)
    
    #pulls UUID from the JSON file, checks status as incomplete or complete, then donwloads, updates the status to complete or pass if complete
    meta_appended = getJSON_read(args.s3_meta_append_fpath)
    for each in meta_appended:
        only_uuid = each['properties']['uuid']
        try:
            imars_etl.select('WHERE uuid="{}"'.format(
                only_uuid
            ))
            file_exists = True
        except imars_etl.exceptions.NoMetadataMatchException.NoMetadataMatchException:
            file_exists = False
        # if not file_exists:
        if file_exists:  # and each['properties']['status'] == 'Incomplete':
            download_metadata = api.download(only_uuid)
            summary_str = each['properties']['summary']
            assert summary_str.startswith('Date: ')
            date_time = summary_str.split(',')[0].split('Date: ')[1].replace('Z', '').replace('T', ' ')
            print('granule date_time: ' + date_time)
            AREA_ID = 12
            L1_PRODUCT_ID = 36
            imars_etl.load(
                download_metadata['path'],
                sql="uuid='{}' AND date_time='{}' AND product_id={} AND area_id={} AND provenance='s3_api_v1'".format(
                    only_uuid, date_time, L1_PRODUCT_ID, AREA_ID
                ),
            )
            # if imars-etl has trouble could use:
            # bash `mv "./*.zip" "/srv/imars-objects/ftp-ingest/."`
            #    in python: os.move shutil.move
            each['properties'].update({'status':'Complete'})
            with open(args.s3_meta_append_fpath,'w') as outfile:
                json.dump(meta_appended,outfile)
        else:
            pass

if __name__ == "__main__":
    parser = ArgumentParser(description='short desc of script goes here')
    parser.add_argument("-g", "--geojson", dest="roi_geojson_fpath", help="florida geojson fpath")
    parser.add_argument("-m", "--meta", dest="metadata_s3_fpath", help="pass in the metadata_s3_fpath")
    parser.add_argument("-a", "--append", dest="s3_meta_append_fpath", help="pass in appended meta_s3_fpath")
    main(parser.parse_args())
