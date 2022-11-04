# Test Placekey.io

## Assigned to:
- github_user

## Description
This is a reach goal. Using Placekey.io is likely not required to get the results we are looking for, however it may add additional fidelity and utlity to the dataset if implemented.

## Requirements

### Objectives

- For every address record, extend HSDS with a field for the `placekey`.
- Determine whether this step should run before or after dedupclication. It may improve matching results if it's close to 100% finding the correct placekey _before_ matching.

### Constraints
Make a list of the limitations/things the code can _not_ do.
- 

## Resources
Insert all the materials needed to complete this code.
- Placekey docs (https://docs.placekey.io/)

[Examples (python):](https://pypi.org/project/placekey/)

```
>>> place = {
>>>   "street_address": "598 Portola Dr",
>>>   "city": "San Francisco",
>>>   "region": "CA",
>>>   "postal_code": "94131",
>>>   "iso_country_code": "US"
>>> }
>>> pk_api.lookup_placekey(**place, strict_address_match=True)
{'query_id': '0', 'placekey': '227@5vg-82n-pgk'}
```

```
>>> places = [
>>>   {
>>>     "street_address": "1543 Mission Street, Floor 3",
>>>     "city": "San Francisco",
>>>     "region": "CA",
>>>     "postal_code": "94105",
>>>     "iso_country_code": "US"
>>>   },
>>>   {
>>>     "query_id": "thisqueryidaloneiscustom",
>>>     "location_name": "Twin Peaks Petroleum",
>>>     "street_address": "598 Portola Dr",
>>>     "city": "San Francisco",
>>>     "region": "CA",
>>>     "postal_code": "94131",
>>>     "iso_country_code": "US"
>>>   },
>>>   {
>>>     "latitude": 37.7371,
>>>     "longitude": -122.44283
>>>   }
>>> ]
>>> pk_api.lookup_placekeys(places)
[{'query_id': 'place_0', 'placekey': '226@5vg-7gq-5mk'},
 {'query_id': 'thisqueryidaloneiscustom', 'placekey': '227-222@5vg-82n-pgk'},
 {'query_id': 'place_2', 'placekey': '@5vg-82n-kzz'}]
```



### Areas of interest
- ? location_name is interesting => What do these yield? 
```
Work Opportunities, Inc.
Bellingham Food Bank
Bellingham Public Library
Blue Skies for Children?
```

- GIVEN a partial address (ex. missing city & state, zip and street only), is a placekey found?
- GIVEN a source with both an Address and lat/lng is THE SAME placekey found?
- GIVEN two sources with variant lat/lng (ex. 4th decimal different, or 6 deciamls vs 4, etc ) is  the same placekey found?
  - OR _How much lat/lng variance produces a DIFFERENT placekey?_

## Documentation Protocol
First, include a readme that describes what your code does and explains difficult bits.

Second, use descriptive variables and method names so that the code is readable and obvious.

Third, comment each function or method along with inputs and returns, and use other inline comments to make particularly opaque or unavoidably clever code clear.

A story is absolutely __not__ complete until time has been spent at the end revewing, updating, and tidying up documentation.

## Get Help
Questions? Check around to see who's available, and ask:
- cskyleryoung
- devcshort
- greggish

### Washington State Resource Data SiloBuster 08/2022