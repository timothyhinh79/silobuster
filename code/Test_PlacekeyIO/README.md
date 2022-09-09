# PLACEKEY TESTING

_[Placekey](https://www.placekey.io/) is a free, universal standard identifier for any physical place, so that the data pertaining to those places can be shared across organizations easily._

## Under what circumstances does Placekey return expected results?

Given a standard address **OR** lat/lng with `iso_country_code`, the service returns an expected Placekey consistently. _see Manual Results, below_

This includes :
- a **missing** street designation in `street_address` (Street / Road / Ave, etc.) (`strict_address_matching = false`)
- a **completely incorrect** street designation (_Boulevard_ replaces _Rd_ in test)
- a **missing `city`**
- a **missing `postal_code`**

**This might allow for some very dirty records to find a successful PK**

## Does the Placekey service yield consistent and useful results for our particular data?

### For making a hard match on duplicates
Not determined. We probably want a known match set (even a pretty small one) to run for comparison.

### For adding extra data for comparison
Certainly :
1. `x_placekey` generated from even dirty, incomplete addresses may very well yield exact matches
2. additionally, adding `x_placekey_latlng` when present in source would give an additional point of comparison (`lat/lng` vs. `address` was not consistent in this testing)
3. The PlaceKey library also provides distance calculations (unexplored here)
  - given some tolereance threshold, it seems like two 'close' PKs might find some fraction of additional confidence
  - this might only be needed for lat/lng in dedupe, but there's real potential later for filtering and more 


## TODO

- [ ] Locate and test variant lat/lng  - _How much deviation yields a different Placekey?_
- [ ] Run a list of known matches for PK comparisons.
- [ ] Do we have consensus on Placekey being worthwhile?
  
### If consensus is reached :
  - [ ] A schema migration adds `x_placekey` and/or `x_placekey_latlng` to _location_ or __addresses_ or _new_table_
  - [ ] Python queries DB
  - [ ] Python logs metadata
  - [ ] Python writes `x_placekey` to record


## REQUIRED

### 1. `pip install placekey`
[See also NODE](https://github.com/Placekey/placekey-js)

### 2. A Placekey API key.

You must sign up for a (free) account.

https://dev.placekey.io/default/register
#### `pip install -U python-dotenv`
#### see .env.template



## MANUAL RESULTS

- Usually, `postal_code` or `longitude` is required.
- `location_name` presence is indeterminate (for this record)
- `strict_address_match` works as expected, allowing for some missing
  - `postal_code` seems universally required, regardless
  - no effect on lat/lng query
- **full address** != **lat/lng** (boo)


### full_address
```
{'city': 'Acme',
 'iso_country_code': 'US',
 'postal_code': '98220',
 'query_id': 'full_address',
 'region': 'WA',
 'street_address': '5200 Turkington Rd'}
```
 
|Query|Strict match?|Result|
|---|---|---|
| full_address | False | 222@5x2-z9t-f4v |
| full_address | True | 222@5x2-z9t-f4v |
 
### latng
```
{'latitude': 48.71876, 'longitude': -122.208755, 'query_id': 'latng'}
```
 
|Query|Strict match?|Result|
|---|---|---|
| latng | False | @5x2-z9t-f2k |
| latng | True | @5x2-z9t-f2k |
 
### removed_road
```
{'city': 'Acme',
 'iso_country_code': 'US',
 'postal_code': '98220',
 'query_id': 'removed_road',
 'region': 'WA',
 'street_address': '5200 Turkington'}
```
 
|Query|Strict match?|Result|
|---|---|---|
| removed_road | False | 222@5x2-z9t-f4v |
| removed_road | True | Address found but is not an exact match |
 
**_caveat_** : _A New York City address of `333 6TH` is not going to be this lucky...._

### road_as_boulevard
```
{'city': 'Acme',
 'iso_country_code': 'US',
 'postal_code': '98220',
 'query_id': 'road_as_boulevard',
 'region': 'WA',
 'street_address': '5200 Turkington Boulevard'}
```
 
|Query|Strict match?|Result|
|---|---|---|
| road_as_boulevard | False | 222@5x2-z9t-f4v |
| road_as_boulevard | True | Address found but is not an exact match |
 
### nocity
```
{'iso_country_code': 'US',
 'postal_code': '98220',
 'query_id': 'nocity',
 'region': 'WA',
 'street_address': '5200 Turkington Rd'}
```
 
|Query|Strict match?|Result|
|---|---|---|
| nocity | False | 222@5x2-z9t-f4v |
| nocity | True | Address found but is not an exact match |
 
### nozip
```
{'city': 'Acme',
 'iso_country_code': 'US',
 'query_id': 'nozip',
 'region': 'WA',
 'street_address': '5200 Turkington Rd'}
```
 
|Query|Strict match?|Result|
|---|---|---|
| nozip | False | 222@5x2-z9t-f4v |
| nozip | True | Address found but is not an exact match |
 
### location_name
```
{'iso_country_code': 'US',
 'location_name': 'Acme Elementary School',
 'postal_code': '98220',
 'query_id': 'location_name',
 'region': 'WA',
 'street_address': '5200 Turkington Rd'}
```
 
|Query|Strict match?|Result|
|---|---|---|
| location_name | False | 222@5x2-z9t-f4v |
| location_name | True | Address found but is not an exact match |
