# Architect Contributor Metadata
  
## Assigned to:
- github_user

## Description
This is an architecture story. Design and document how to store meta data about _who_ is contributing and to _which_ data records.
  

## Requirements

### Objectives

- Design and document a schema such that the system will thoroughly and efficiently track who has contributed to each cononical record, and which data they contributed.
- This may eventually tie into permissions and roles for access as well. Good idea or no?
- As much as possible this should be efficient to run. Considerations may include whether to store data in nested JSON (PostGRES) or in tables, the cost of join tables, etc.

### Constraints
Make a list of the limitations/things the code can _not_ do.
- 

## Resources
Insert all the materials needed to complete this code.
- name (url)

## Documentation Protocol
First, include a readme that describes what your code does and explains difficult bits.

Second, use descriptive variables and method names so that the code is readable and obvious.

Third, comment each function or method along with inputs and returns, and use other inline comments to make particularly opaque or unavoidably clever code clear.

A story is absolutely __not__ complete until time has been spent at the end revewing, updating, and tidying up documentation.
README --
Modules
  Modules are run in batch mode.  Each run results in a new runs table record and a set of analysis records.
  
datascrubbing module - identifies and removes syntax errors at record/field level
  Data scrubbing module will normalize and validate a set of fields of each record.  No comparison between records.  Any record with a field that cannot be normalized will be flagged with record and field ID.  Fields should be scrubbed that are selected for machine learning in the dedup module. types of scrubbing are: free text, numeric, fixed syntax and enum. Candidate fields to scrub will be:  
 organization
    name
    email
    taxid
    url
  location
    name
    longitude
    latitude
  service
    name
    description
    url
  phone
    number
	physical_address
    address_1
    address_2
    city
    state_province
    postal_code
  postal_address
    address_1
    address_2
    city
    state_province
    postal_code

dedupanalysis module - creates dedupgroups with nominations for deduplication of records.
  Dedupe module will identify agency/organization records that refer to a common agency from more than one source.  The modules will establish the likelihood the record is a duplicate based on a set of (semi-)immutable fields (name, address, phone, domain, site locations, etc). The module will store meta-data to identify groups that might belong together along with a dedupe distance metric and the fields measured and module that produced it.  
Each time the module is run it will create a set of candidate “dupegroups”. These can be manually validated and a protocol for manual and future automated selection of canonical data established.
  
Tables
  contributors
    contributorid	int pk
    contributorname	string

  fields
    fieldid pk
    fieldname

  scrubbingruns
    runid int pk
    start datetime
    end datetime
    contributors json
    fields json 
    recordsrun int
    errorcode int 	0=OK

  scrubbingresults
    resultid int  pk
    runid int	fk
    resultdatestamp datetime
    contributorid int fk
    recordid int	fk
    fieldname string
    errorcode int fk
    errorname string
    before string
    after	string
    fixed bool
  
  deduperuns
    runid int	pk
    start datetime
    end datetime
    contributors json {[fcontributorid: int fk]}
    fields json {[fieldid: int fk]}
    recordsrun int
    errorcode int 	0
  
  deduperesults
    dupegroupid int pk
    runid int fk
    analysisresults json
	    { TBD on reporting and requirement initially: recordid and dedupe score}
    groupstatus string 	(TBD group workflow from new to closed)

 errors type enum
  0 OK
  1 Leading, trailing and duplicate blanks
  2 Missing format markers (e.g. “@”,”.”,”-“)
  3 Capitalization errors
  4 Wrong Character Type (alpha, numeric, special, non-printing)
  5 Mismatched delimiters
  6 Not in enum list
  7 Fixed Format error (url, email, zip)
  8 Foreignkey error
  9 Unknown
  

## Get Help
Questions? Check around to see who's available, and ask:
- cskyleryoung
- devcshort
- greggish

### Washington State Resource Data SiloBuster 08/2022
