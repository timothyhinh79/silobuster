# Architect Contributor Metadata.

Original work provided by Eric Hummel. This story turned into a large question of how to store general metadata related to workflow steps, contributors, and results from workflow steps such that we can maintain state for the import/cleanup/dedupe process and generate useful reports.

## Modules
Modules are run in batch mode. Each run results in a new runs table record and a set of analysis records.
  
### Datascrubbing Module
Identifies and removes syntax errors at record/field level.

Data scrubbing module will normalize and validate a set of fields of each record.  No comparison between records.  Any record with a field that cannot be normalized will be flagged with record and field ID.  Fields should be scrubbed that are selected for machine learning in the dedup module. Types of scrubbing are: free text, numeric, fixed syntax and enum.

Candidate fields to scrub will be:

`organization`
- name
- email
- taxid
- url

`location`
- name
- longitude
- latitude

`service`
- name
- description
- url

`phone`
- number

`physical_address`
- address_1
- address_2
- city
- state_province
- postal_code

`postal_address`
- address_1
- address_2
- city
- state_province
- postal_code

## Dedup Analysis Module
Creates dedupgroups with nominations for deduplication of records.

Dedupe module will identify agency/organization records that refer to a common agency from more than one source.  The modules will establish the likelihood the record is a duplicate based on a set of (semi-)immutable fields (name, address, phone, domain, site locations, etc). The module will store meta-data to identify groups that might belong together along with a dedupe distance metric and the fields measured and module that produced it.

Each time the module is run it will create a set of candidate “dupegroups”. These can be manually validated and a protocol for manual and future automated selection of canonical data established.
  
### Tables
`contributors`
contributorid	int         pk
contributorname	string

`fields`
field_id        int         pk
field_name      string

`scrubbingruns`
runid           int         pk
start           datetime
end             datetime
contributors    json
fields          json 
recordsrun      int
errorcode       int 	    0=OK

`scrubbingresults`
resultid        int         pk
runid           int	        fk
resultdatestamp datetime
contributorid   int         fk
recordid        int	        fk
fieldname       string
errorcode       int         fk
errorname       string
before          string
after	        string
fixed           bool
  
`deduperuns`
runid           int	        pk
start           datetime
end             datetime
contributors    json        {[fcontributorid: int fk]}
fields          json        {[fieldid: int fk]}
records         int
errorcode       int 	    0
  
`deduperesults`
dupegroupid     int         pk
runid           int         fk
analysisresults json        (TBD on reporting and requirement initially: recordid and dedupe score)
groupstatus     string 	    (TBD group workflow from new to closed)

`errors` type enum
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