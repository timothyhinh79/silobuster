import placekey as pk
from placekey.api import PlacekeyAPI

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
	"query_id": "sb_full_address", # this is arbitrary and caller set. Handy!
	"street_address": "5200 Turkington Rd",
	"city": "Acme",
	"region": "WA",
	"postal_code": "98220",
	"iso_country_code": "US"
}

## Nicely responsive validation, thank you placekey!
# => {'message': 'parameter query validation failed: object needs one of the following corrections: parameter iso_country_code is required ; parameter longitude is required ; '}
print("## TEST CASE: Full Address =>")
result_full = pk_api.lookup_placekey(**test_addr_1, strict_address_match=True)
print(result_full)

## TEST CASE: Lat/Lng 
 # SELECT * FROM "within_location" WHERE id = 852;
test_latlng_1 = {
	"query_id": "sb_latlng",
	"latitude": 48.71876,
	"longitude": -122.208755,
}
print("## TEST CASE: Lat/Lng  => ")
result_latlng = pk_api.lookup_placekey(**test_latlng_1, strict_address_match=True)
print(result_latlng)

print("## TEST CASE: Lat/Lng (no strict) => ")
result_latlng_ns = pk_api.lookup_placekey(**test_latlng_1, strict_address_match=False)
print(result_latlng_ns)

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
print("## TEST CASE: Partial Address (no city) => ")
result_partial = pk_api.lookup_placekey(**nocity, strict_address_match=False)
# {'query_id': '0', 'error': 'Address found but is not an exact match'} (strict)
print(result_partial)

## TEST CASE: Partial Address (no zip)
nozip = {
  "query_id": "nozip",
	"street_address": test_addr_1['street_address'],
	"city": test_addr_1['city'],
	# "postal_code": test_addr_1['postal_code'],
	"region": test_addr_1['region'],
	"iso_country_code": test_addr_1['iso_country_code']
}
print("## TEST CASE: Partial Address (no zip) => ")
result_partial2 = pk_api.lookup_placekey(**nozip, strict_address_match=False)
# {'message': 'parameter query validation failed: object needs one of the following corrections: parameter postal_code is required ; parameter longitude is required ; '}
print(result_partial2)


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
print("## TEST CASE: Location Name => ")
result_location = pk_api.lookup_placekey(**location_name, strict_address_match=False)
print(result_location)

