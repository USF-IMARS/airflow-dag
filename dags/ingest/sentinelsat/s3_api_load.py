#!/bin/env python
# connect to the API
# https://buildmedia.readthedocs.org/media/pdf/sentinelsat/master/sentinelsat.pdf for help
#getting it work on mobaxterm: source venv/bin/activate
#if first time start with: export SLUGIFY_USES_TEXT_UNIDECODE=yes; virtualenv venv; source venv/bin/activate; pip install -e .[test]; py.test -v
#might have to add: pip install requests-mock; pip install rstcheck; pip install geojson

from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
from datetime import date
import os
import collections
import json

api = SentinelAPI("user", "pass.", "https://scihub.copernicus.eu/dhus") ##### should we use a general IMARS password and user?
data_dir = os.getcwd()

#Definitions that either read or write JSON files, read needs file name, write needs a variable to write to a file name
def getJSON_read(filePathandName):
	with open(filePathandName,'r') as infile:
		return json.load(infile)

def getJSON_write(filePathandName,variables):
	with open(filePathandName,'w') as outfile:
		return json.dump(variables,outfile)

def remove_dupes(mymetalist):
    newlist = []
    for item in mymetalist:
        data_exist = False
        for ud in newlist:
            if ud['properties'] == item['properties']:
                data_exists = True
                break
            else:
                newlist.append(item)



#starts new list, checks if each JSON file has data, then adds old data and new data, then overwrites previous JSON file
new_meta=[]
if os.stat('metadata_s3.json').st_size == 0:
	if os.stat('metadata_s3_appended.json').st_size == 0:
		exit()
	else:
		old_meta = getJSON_read('metadata_s3_appended.json')
		new_meta.extend(old_meta)
else:
	metadata = getJSON_read('metadata_s3.json')
	if os.stat('metadata_s3_appended.json').st_size == 0:
		new_meta.extend(metadata)
	else:
		old_meta = getJSON_read('metadata_s3_appended.json')
		new_meta.extend(old_meta)
		new_meta.extend(metadata)

#TODO set a way to check for duplicates that would inserted into line 55
#attemps are def remove_dupes and line 66-74

#old_meta = getJSON_read('metadata_s3_appended.json')
#metadata = getJSON_read('metadata_s3.json')
#new_meta.extend(old_meta)
#new_meta.extend(metadata)
#print(new_meta)


#new_metas = remove_dupes(new_meta)
#newslist = []
#for item in new_meta:
#    newlist = []
#    newlist = item['properties']['uuid']
#    if newlist not in new_meta['properties']['uuid']:
#        newslist.append(item)
#        print(newslist)

getJSON_write('metadata_s3_appended.json',new_meta)

#pulls UUID from the JSON file, checks status as incomplete or complete, then donwloads, updates the status to complete or pass if complete
meta_appended = getJSON_read('metadata_s3_appended.json')
for each in meta_appended:
	only_uuid = each['properties']['uuid']
	# try:
	# 	imars_etl.select('WHERE uuid="{}"'.format(
	# 		only_uuid
	# 	))
	# 	file_exists = True
	# except imars_etl.exceptions.NoMetadataMatchException.NoMetadataMatchException:
	# 	file_exists = False
	# if not file_exists:
	if each['properties']['status']== 'Incomplete':
		#download_metadata = api.download(only_uuid)			#will need to be uncommented once have user and pass inserted
			# TODO:
		# import imars_etl
		# imars_etl.load(
		# 	download_metadata['path'],
		#	sql="uuid='{}' AND date_time='{}'".format(
		# 		only_uuid
		# 	)
		# )
	# bash `mv "./*.zip" "/srv/imars-objects/ftp-ingest/."`
	#    in python: os.move shutil.move
		each['properties'].update({'status':'Complete'})
		with open('metadata_s3_appended.json','w') as outfile:
			json.dump(meta_appended,outfile)
	else:
		pass
