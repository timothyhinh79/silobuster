# Architecture Notes

## Workflow
At this event, we are creating and documenting a bulk data ingestion workflow, orchestrated by Airflow, that will (in order):
1. Ingest / transform data.
2. Run data quality checks and store metadata (useful before/after reports).
3. Sanitize and Normalize all fields.
4. Run duplicate checks and store metadata.
5. Report on quality and overlap of data sets by contributor.