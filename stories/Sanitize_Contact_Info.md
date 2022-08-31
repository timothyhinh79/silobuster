# Sanitize Contact Info

## Assigned to:
- github_user

## Description
Both for quality of data and ease of matching records, phone number, emails, and URLs should be coerced into a standard, machine readable format.

## Requirements

### Objectives
Make a bullet list of the things that the code must do.
- Recognize data in many different formats.
- Be "on-the-fly" configurable to watch for custom formats (ie, pass in regex as args?).
- Recognize multiple phone numbers in an entry and parse them into separate rows in HSDS.
- Recognize supplemental data and store in appropriate fields, ie phone extensions in  `phone.extention`.
- If the `type` for a phone record cannot be determined, flag for review by data manager.
- Recognize multiple emails or urls in an entry and pull out the first one to store.
- Coerce all data into ITU E.123 format for international numbers.

### Constraints
Make a list of the limitations/things the code can _not_ do.
- 

## Resources
Insert all the materials needed to complete this code.
- Easy docs for E.123 (https://en.wikipedia.org/wiki/E.123)
- Full docs for E.123 (https://www.itu.int/rec/T-REC-E.123-200102-I/en)

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