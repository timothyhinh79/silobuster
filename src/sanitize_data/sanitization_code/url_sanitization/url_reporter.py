import sqlalchemy
import pandas as pd

class URL_Reporter:
    def __init__(self, log_db, log_table, job_id):
        self._log_db = log_db
        self._log_table = log_table
        self._job_id = job_id

        self._logs_parsed = self._parse_logs()

    @property
    def log_db(self):
        return self._log_db

    @property
    def log_table(self):
        return self._log_table

    @property
    def job_id(self):
        return self._job_id

    @property
    def logs_parsed(self):
        return self._logs_parsed

    # NOTE: consider converting SQL query into Python logic
    def _parse_logs(self):
        """Retrieves log records for given job and adds extra fields for reporting"""
        db = sqlalchemy.create_engine(self.log_db)
        conn = db.connect()

        # adding extra fields that will be helpful for reporting (e.g. URL status codes, whether string contained URLs or not, etc.)
        conn_exec = conn.execute(sqlalchemy.sql.text(f"""
            WITH logs_w_prompts AS (
                SELECT * 
                    , (log_message->'prompts'->0->'description')::VARCHAR as first_prompt
                    , CASE WHEN json_array_length(log_message->'prompts') > 1 THEN 
                        (log_message->'prompts'->1->'description')::VARCHAR
                        ELSE '' END as second_prompt
                FROM {self.log_table}
                WHERE step_name = 'sanitize_url' AND job_id = :job_id
            )
            SELECT 
                  id
                , job_id
                , iteration_id
                , step_name
                , contributor_name
                , substring(first_prompt from '"([\w\s]+)') as url_condition
                , CASE WHEN substring(first_prompt from '[sS]anitized ''[^'']*'' to ''[^'']*''') IS NOT NULL THEN 'Changed' ELSE 'Not Changed' END AS sanitization_change
                , substring(second_prompt from '[fF]ull URL ''([^'']+)'' (?\:is not valid )?\((?\:generates no response|returns a \d{{3}} status code)\)') AS full_URL
                , CASE WHEN second_prompt LIKE '%Received error code%' THEN 'Received 429/503 status, need to check URL again '
                    ELSE substring(second_prompt from '[fF]ull URL ''[^'']+'' (?\:is not valid )?\((generates no response|returns a \d{{3}} status code)\)') END AS full_URL_status
                , substring(second_prompt from '[rR]oot URL ''([^'']+)'' (?\:(?\:is not valid )?\((?\:generates no response|returns a \d{{3}} status code)\)|is valid)') AS root_URL
                , CASE WHEN second_prompt LIKE '%Received error code%' THEN 'Received 429/503 status, need to check URL again '
                    ELSE REGEXP_REPLACE(
                        substring(second_prompt from '[rR]oot URL ''[^'']+'' ((?\:(?\:is not valid )?\((?\:generates no response|returns a \d{{3}} status code)\)|is valid))'),
                        '[\(\)]', '', 'g'
                    ) END AS root_URL_status
                
            FROM logs_w_prompts;
        """), job_id = self.job_id)


        # convert tuples into JSON with column headers
        cols = conn_exec.keys()
        results = [{col:val for col, val in zip(cols, vals_tuple)} for vals_tuple in conn_exec.fetchall()]

        conn.close()
        db.dispose()

        # convert JSON into Dataframe
        return pd.DataFrame(results)

    ### METHODS TO GENERATE REPORTS
    # def generate(self):
    #     breakpoint()
    #     url_condition_summary = self.logs_parsed.groupby('url_condition', as_index=False).agg(num_records=('id', 'count')) 
    #     error_code_summary = self.logs_parsed.groupby('full_url_status', as_index=False).agg(num_records=('id', 'count')) 
    #     sanitization_summary = self.logs_parsed.groupby('sanitization_change', as_index=False).agg(num_records=('id', 'count')) 


    # def error_code

    

    