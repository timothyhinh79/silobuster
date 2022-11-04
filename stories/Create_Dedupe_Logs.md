# Create Reports

## Assigned to:
- github_user

## Description

We've had eyes on what Dedupe.io (or rather PandasDedupe in this case) outputs as dedupe metadata. 

## Requirements

### Objectives

- Create Log table if it doesn't already exist.
- Define the JSON shape for logging dupe data.
- Wire up the ability to export logs from the dedupe step.

The output from `pandas-dedupe` is a CSV stored at the root of the project folder where run. The logging step will include:
- Parsing the CSV
- Adding additional information (foreign keys for our DB, for example).
- Writing results to the `log` table in our POC.

The shape of data will be:

```json
{
    "id": "uuid",
    "job_id": "string",
    "log_type": "string",
    "log": "string"
}
```

### Constraints

- Logs must remain in a consistent format.

## Resources
Insert all the materials needed to complete this code.
- name (url)

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