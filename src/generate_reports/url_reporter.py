import sqlalchemy 
import pandas as pd # for generating reports as DataFrame tables
import os

class URL_Reporter:
    def __init__(self, log_db, log_table, job_id, output_folder = None):
        self._log_db = log_db
        self._log_table = log_table
        self._job_id = job_id
        if output_folder:
            self._output_folder = output_folder
        else:
            self._output_folder = f"url_reports_{job_id}"

        self._logs_parsed = self._parse_logs()
        try:
            assert self.logs_parsed['total_records'].max() == self.logs_parsed['total_records'].min()
            self._total_records = self.logs_parsed['total_records'].max()
        except:
            raise Exception('minimum value of "total_records" is not equal to maximum value ("total_records" should only have one value)')

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

    @property
    def total_records(self):
        return self._total_records

    @property
    def output_folder(self):
        return self._output_folder

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
                , job_timestamp
                , total_records
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

    def url_condition_summary(self):
        """Generates DataFrame summarizing URL conditions (e.g. 'String is URL', 'String is invalid URL', 'String contains no URLs') """
        url_condition_summary = self.logs_parsed.groupby('url_condition', as_index=False, dropna = False).agg(num_records=('id', 'count'))
        url_condition_summary['url_condition'][url_condition_summary['url_condition'] == 'String is URL'] = 'String is invalid URL'
        url_condition_summary = pd.concat([url_condition_summary, 
                                           pd.DataFrame([{'url_condition': 'String is valid URL', 
                                                          'num_records': self.total_records - url_condition_summary['num_records'].sum()}])])                                                         
        return url_condition_summary
    
    def status_code_summary(self):
        """Generates DataFrame summarizing distribution of URL status codes """
        status_code_summary = self.logs_parsed.groupby('full_url_status', as_index=False, dropna = False).agg(num_records=('id', 'count')) 
        status_code_summary['full_url_status'][status_code_summary['full_url_status'].isna()] = 'No URL detected'
        status_code_summary = pd.concat([status_code_summary,
                                        pd.DataFrame([{'full_url_status': 'Valid status code', 'num_records': self.total_records - status_code_summary['num_records'].sum()}])])
        return status_code_summary

    def sanitization_summary(self):
        """Generates DataFrame summarizing sanitization results (how many records were modified) """
        sanitization_summary = self.logs_parsed.groupby('sanitization_change', as_index=False, dropna = False).agg(num_records=('id', 'count')) 
        sanitization_summary['num_records'][sanitization_summary['sanitization_change']=='Not Changed'] += self.total_records - sanitization_summary['num_records'].sum()

        return sanitization_summary

    def write_reports(self):
        """ Write reports to CSV files in designated folder """
        # generate DataFrame tables
        url_condition_summary = self.url_condition_summary()
        status_code_summary = self.status_code_summary()
        sanitization_summary = self.sanitization_summary()

        # create designated folder for outputting tables to CSV, if it doesn't already exist
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
            print(f'New directory {self.output_folder} created')

        # export DataFrames to CSV in designated folder
        url_condition_summary.to_csv(self.output_folder + '/url_condition_summary.csv', index = False)
        status_code_summary.to_csv(self.output_folder + '/status_code_summary.csv', index = False)
        sanitization_summary.to_csv(self.output_folder + '/sanitization_summary.csv', index = False)
    

    