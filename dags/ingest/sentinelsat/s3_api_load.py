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
#import pandas as pd
import json

api = SentinelAPI("user", "pass.", "https://scihub.copernicus.eu/dhus") ##### should we use a general IMARS password and user? 
data_dir = os.getcwd()

# download single scene by known product id
#api.download(<product id>)
#ex api.download('16e7b752-c1a7-4ea0-8107-756005d6c29a') 

	# search by polygon, time, and SciHub query keywords
	#footprint = geojson_to_wkt(read_geojson("florida.geojson"))

	#products = collections.OrderedDict()
	#products = api.query(footprint,
	#					date=('20171008', date(2017, 10, 9)), #two different ways to show date
							#also one is start and other is end time
					#area_relation({'Intersects','Contains','IsWithin'}) might need to add, default intersect	
	#					platformname='Sentinel-3',
	#					producttype='OL_1_EFR___')
					
	# convert to Pandas DataFrame
	#products_df = api.to_dataframe(products)
	#print(list(products_df['uuid']))
             
	#import pdb; pdb.set_trace()

# GeoJSON FeatureCollection containing footprints and metadata of the scenes
#jsonee = api.to_geojson(products)
def getJSON(filePathandName):
	with open('metadata_s3.json','r') as infile:
		return json.load(infile)

metadata = getJSON('metadata_s3.json')


#probably wont need this line 49 - 53 as is what line 73 to 77 is doing
#for key, value in metadata['features']:                               #issues, not adding a new dict to the current list, want to add status so that can add 
#	statuss = ({'status' : 'Incomplete'})
#	meta_properties = key['properties']  							  #if statement that is if incomplete will download, if not return
#	meta_appended = meta_properties.insert(38 ,statuss)
#	print(meta_appended)

meta_appended = metadata
#meta_appended['features'][0]['properties']['status'] = 'incomplete'		#I'm looking at still trying to append a dictionary, but it's not promission
#print(meta_appended)
#def getJSON_oldData(filePathandName):
#	with open('metadata_s3_appended.json','r') as infile:
#		return json.load(infile)
#old_meta = getJSON_oldData('metadata_s3_appended.json')

#meta_new = old_meta.extend(meta_appended)
#print(meta_new)

#for item in old_meta['features']:
#	item['features']=meta_appended
#	with open('metadata_s3_appended.json','w') as outfile:
#		json.dump(meta_appended,outfile)
#meta_appended = old_meta
#print(meta_appended)														#this is the end of my effort ^

for item in meta_appended['features']:										#adds status : incomplete to the properties in for each image metadata
	item['properties']['status']='Incomplete'
	with open('metadata_s3_appended.json','w') as outfile:
		json.dump(meta_appended,outfile)
meta_appended = getJSON('metadata_s3_appended.json')


#UUIDs = metadata['features'][1]['properties']['uuid']					#this probably wont be need (line 79-85) as lines 87-94 are a copy of it with added code
#print(UUIDs)
#api.download(UUIDs)
#for each in metadata['features']:                                    #issues: only downloads one image then quits
#	only_uuid = each['properties']['uuid']							  #add if statement that searches for incomplete and complete download in the status, 
#	print(only_uuid)												  #add append to change incomplete status to complete once downloaded
#	api.download(only_uuid)

#api.download(UUIDs)												  #pulls UUID from the JSON file that has added status, downloads, then updates the status to complete
for each in meta_appended['features']:                                    
	only_uuid = each['properties']['uuid']							  #add if statement that searches for incomplete and complete download in the status, 												  
	api.download(only_uuid)
	each['properties'].update({'status':'Complete'})	
	with open('metadata_s3_appended.json','w') as outfile:
		json.dump(meta_appended,outfile)