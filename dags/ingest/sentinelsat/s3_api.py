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
#import pandas as pd  # not needed as I haven't used Pandas for a dataframe
import json

api = SentinelAPI("user", "pass.", "https://scihub.copernicus.eu/dhus") ##### should we use a general IMARS password and user? 
data_dir = os.getcwd()                                                 # the only way I found to get all the parts of code to work in my directory

# download single scene by known product id							   #used if downloading one image using the UUID
#api.download(<product id>)
#ex api.download('16e7b752-c1a7-4ea0-8107-756005d6c29a')

# search by polygon, time, and SciHub query keywords				   #where the query starts, GEOJson focuses on florida
footprint = geojson_to_wkt(read_geojson("florida.geojson"))

products = collections.OrderedDict()
products = api.query(footprint,
					date=('20171025', date(2017, 10, 30)), 			   #two different ways to show date, once we get it going, change the first date to '20150101' and last to 'NOW', then update to be 'NOW-1 and 'NOW'
					#area_relation({'Intersects','Contains','IsWithin'}) might need to add, default intersect	#propbably wont need
					platformname='Sentinel-3',
					producttype='OL_1_EFR___',
					productlevel= 'L1')
																	   #their are other variable we can add, say if we also want S2 images or another product from S3 
# download all results from the search, not using in this script
#api.download_all(products)

# convert to Pandas DataFrame										   #tried using this in past, but went to JSON 
#products_df = api.to_dataframe(products)
#print(list(products_df['uuid']))
             
#import pdb; pdb.set_trace()										   #just testing a break, but ended not needing

# GeoJSON FeatureCollection containing footprints and metadata of the scenes		#how I get query data to JSON format and into JSON file
json_query_results = api.to_geojson(products)
with open('metadata_s3.json','w') as outfile:
	json.dump(json_query_results,outfile)


														   # possible todo: add appending loop so if ran multiple times adds to the list of metadata instead of starting over;
														   #will need this when add a status variable, thinking about  

####the rest probably wont need, it was me trying a few different things with JSON files and PANDAS, mostly just printing specific metadata 
#jsonee= api.to_geojson(products)
#meta_short=[]
#for x in jsonee:
	

#	meta=x('uuid')
#	meta_list.append(meta)

#print(meta_list)
#print(jsonee['features']['properties']['uuid'])
#with open('metadata_s3_uuid.json','w') as outfile:
#	json.dump(jsonee,outfile)

# GeoPandas GeoDataFrame with the metadata of the scenes and the footprints as geometries
#api.to_geodataframe(products)

# Get basic information about the product: its title, file size, MD5 sum, date, footprint and
# its download url
#uuids_from_list=((list(products_df['uuid'])))
#meta_short= []
#for x in uuids_from_list:
	
#	meta= api.get_product_odata(x)
#	meta_short.append(meta)
#print(meta_short)


#to_csv1 = products_df.to_csv(index=True) 
# Get the product's full metadata available on the server
#api.get_product_odata(<product_id>, full=True)
