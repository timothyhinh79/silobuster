# Test Dedupe.io

## Assigned to:
- github_user

## Description
This is one of the larger and more general stories: Dedupe.io is a sophisticated, open source library for matching duplicates, including descriptions, names, and even addresses. We need to test Dedupe.io and find out how effective it is for our use case, and for which kinds of data.

For example, Dedupe.io might be the only library we need to check for duplicates across all data present. _Or_, it might be excellent for text fields, but less for so addresses, in which case we'll attempt to suplement it with Placekey.io or other options.

We are fairly certain it will at least make an excellent backbone for first-pass duplicate detection, so this will likely be the primary deduplication step.

## Requirements

### Dependencies
- A sample database with duplicate records.

### Objectives

- Analyze records and store data about potential matches.
- Determine the best way to store data about matches. In a separate table with a join table? Stored on each record itself? Using JSON storage in PostGRES?
- Determine and flag a default canonical version of each record. Ie, picking by contributor, earliest creation date, highest quality data, etc.
- Determine how best to identify resolved matches.
- Deduplicate the `organization` and `location` core tables.
- Attempting deduplication of the `service` core table should be considered a secondary priority and reach goal. There are extenuating factors for that table.

### Constraints
Make a list of the limitations/things the code can _not_ do.
- 

### Potential edge cases and questions
- If, after data changes for two _already canonical_ records those records get matched as duplicates, how do we resolve the new canonical record?
- After merging records into a canonical record, should future dedupes run against _all_ stored source records, or start fresh with only the canonical records? Keep in mind that the source records may get stale over time.

## Resources
Insert all the materials needed to complete this code.
- Proposal for ServiceNet 2.0 (https://docs.google.com/presentation/d/1iV_RWk37NtC5cCjCrL8UJdJwlpQIiL-Gbr6jXoYjMoI/edit#slide=id.gcb6320ac8a_3_117)
- Original source code for SErviceNet. Don't get too lost in here, looks like it was largely checking hashes for exact matches. (https://github.com/benetech/ServiceNet/tree/dev/src/main/java/org/benetech/servicenet/matching)

## Documentation Protocol
First, include a readme that describes what your code does and explains difficult bits.

Second, use descriptive variables and method names so that the code is readable and obvious.

Third, comment each function or method along with inputs and returns, and use other inline comments to make particularly opaque or unavoidably clever more code clear.

A story is absolutely __not__ complete until time has been spent at the end revewing, updating, and tidying up documentation.

## Get Help
Questions? Check around to see who's available, and ask:
- cskyleryoung
- devcshort
- greggish

### Washington State Resource Data SiloBuster 08/2022