# PLACEKEY TESTING

_[Placekey](https://www.placekey.io/) is a free, universal standard identifier for any physical place, so that the data pertaining to those places can be shared across organizations easily._

## TODO

- [ ] Locate and test variant lat/lng  - _How much deviation yields a different Placekey?_
- [ ] Consensus on whether this is worthwhile :
  - [ ] schema migration adds `x_placekey` to _location_ or __addresses_ or _new_table_
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
```
test_addr_1 = {
	"query_id": "sb_full_address", # this is arbitrary and caller set. Handy!
	"street_address": "5200 Turkington Rd",
	"city": "Acme",
	"region": "WA",
	"postal_code": "98220",
	"iso_country_code": "US"
}
```

```
test_latlng_1 = {
	"query_id": "sb_latlng",
	"latitude": 48.71876,
	"longitude": -122.208755,
}
```


|DATA|strict_address_match|SUCCESS|RESULT|
|--|--|--|--|
|test_addr_1|True|YES|**222@5x2-z9t-f4v**|
|-city|False|YES|**222@5x2-z9t-f4v**|
| -zip |False|NO|_n/a_|
|-city, +location_name|False|YES|**222@5x2-z9t-f4v**|
|test_latlng_1|True|YES|**@5x2-z9t-f2k**|
|test_latlng_1|False|YES|**@5x2-z9t-f2k**|

