# Sanitize Addresses

## Assigned to:
- github_user

## Description
Standardizing addresses will help increase matching fidelity in later steps. We should be able to use the free USPS validation API for this.

## Requirements

### Objectives

- Validate addresses using the free USPS API.
- Fix any errors that come back.
- Flag addresses that are inconclusive.

### Constraints

- Avoid _changing_ provided addresses to a different location. An address may be so poorly formated that it gets mistaken for a different location altogether. This may be unavoidable, but keep it in mind.

## Resources
Insert all the materials needed to complete this code.
- Domestic USPS address validator (https://www.usps.com/business/web-tools-apis/#api)

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