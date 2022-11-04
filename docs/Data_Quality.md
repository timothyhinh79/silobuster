# Data Quality

## Assigned to:
- github_user

## Description
This is more of a thought leadership experiment. How, and to what extent, can we determine data quality at the machine level?

Some thoughts and ideas to start with:
- Data completeness. Ie, if a Service is missing a phone number, it gets a lower score than one that does.
- Data robustness. Ie, perhaps Service A has one number, and Service B has four numbers. Does B get a higher score?
- Data accuracy. Ie, can some fields be check programatically against public APIs for accuracy? Ie, perhaps we can automatically look up `organization.tax_id` and verify it. Which fields can we do this for?
- Data freshness. Ie, how recently was a record updated?

The number of fields present in a given records, as compared with potential duplicates may be one metric. Perhaps the fields can be weighted as well. If so, what are the weights for fields in the core tables?

What else can signal data quality?

## Requirements

### Objectives
Make a bullet list of the things that the code must do.
- Document potential data quality standards. Pleaes be as specific as possible and provide examples.

### Constraints
Make a list of the limitations/things the code can _not_ do.
- 

## Resources
Insert all the materials needed to complete this code.
- ServiceNet Proposal (https://docs.google.com/presentation/d/1iV_RWk37NtC5cCjCrL8UJdJwlpQIiL-Gbr6jXoYjMoI/edit#slide=id.gcb6320ac8a_3_111)

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