import placekey as pk
from placekey.api import PlacekeyAPI
from pprint import pprint
#TODO: move .env to a settings.py & import
from dotenv import load_dotenv
from pathlib import Path
import os
load_dotenv()
env_path = Path('.')/'.env'
load_dotenv(dotenv_path=env_path)
PLACEKEY_API_KEY = os.getenv("PLACEKEY_API_KEY")
# END .env


placekey_api_key = PLACEKEY_API_KEY

pk_api = PlacekeyAPI(placekey_api_key)


# Let's manually pick a record from the data set:

## TEST CASE: Full Address =>
# SELECT * FROM "within_physical_addresse" WHERE location_id = 852;
test_addr_1 = {
	"query_id": "full_address", # this is arbitrary and caller set. Handy!
	"street_address": "5200 Turkington Rd",
	"city": "Acme",
	"region": "WA",
	"postal_code": "98220",
	"iso_country_code": "US"
}

## Nicely responsive validation, thank you placekey!
# => {'message': 'parameter query validation failed: object needs one of the following corrections: parameter iso_country_code is required ; parameter longitude is required ; '}


## TEST CASE: Lat/Lng 
 # SELECT * FROM "within_location" WHERE id = 852;
latlng = {
	"query_id": "latng",
	"latitude": 48.71876,
	"longitude": -122.208755,
}


## TEST CASE: remove_road
# => What if there is no Rd/Ln/St designation in the address? 
noroad = test_addr_1.copy()
noroad['query_id'] = 'removed_road'
noroad['street_address'] = '5200 Turkington'

## TEST CASE: remove_road
# => What if the Rd/Ln/St is innacurate? 
wrongroad = test_addr_1.copy()
wrongroad['query_id'] = 'road_as_boulevard'
wrongroad['street_address'] = '5200 Turkington Boulevard'

## TEST CASE: Partial Address
## Nicely responsive validation, thank you placekey!
## => {'message': 'parameter query validation failed: object needs one of the following corrections: parameter region is required ; parameter longitude is required ; '}
nocity = {
  "query_id": "nocity",
	"street_address": test_addr_1['street_address'],
	# "city": test_addr_1['city'],
	"postal_code": test_addr_1['postal_code'],
	"region": test_addr_1['region'],
	"iso_country_code": test_addr_1['iso_country_code']
}


## TEST CASE: Partial Address (no zip)
nozip = {
  "query_id": "nozip",
	"street_address": test_addr_1['street_address'],
	"city": test_addr_1['city'],
	# "postal_code": test_addr_1['postal_code'],
	"region": test_addr_1['region'],
	"iso_country_code": test_addr_1['iso_country_code']
}

# # {'message': 'parameter query validation failed: object needs one of the following corrections: parameter postal_code is required ; parameter longitude is required ; '}


## TEST CASE: Location Name
# => These are the minimum viable paramaters! 
# - Without City, it's not exact (on strict_address_match=True)!
# - Without City, it's the same as 'test_addr_1' (on strict_address_match=False)!
location_name = {
"query_id": "location_name",
"street_address": test_addr_1['street_address'],
"location_name": "Acme Elementary School",
"postal_code": test_addr_1['postal_code'],
"region": test_addr_1['region'],
"iso_country_code": test_addr_1['iso_country_code']
}



places = [
	test_addr_1,
	latlng,
	noroad,
	wrongroad,
	nocity,
	nozip,
	location_name
]

strict_results = pk_api.lookup_placekeys(places, strict_address_match=True)
results = pk_api.lookup_placekeys(places, strict_address_match=False)

# MARKDOWN OF places, with SArtict/No Strict table
for i, place in enumerate(places):
	print("### %s" % place['query_id'])
	print('```')
	pprint(place)
	print('```')
	print(' ')
	print('|Query|Strict match?|Result|')
	print('|---|---|---|')
	r = results[i]
	msg = r['placekey'] if 'placekey' in r else r['error']
	print("| %s | False | %s |" % (r['query_id'], msg))
	r = strict_results[i]
	msg = r['placekey'] if 'placekey' in r else r['error']
	print("| %s | True | %s |" % (r['query_id'], msg))
	print(' ')
