"""
ApplicationConstants.py
"""

sql_invoice_pipeline_history = """INSERT INTO pipeline_history (etl_name , skip_execution, 
    is_active, full_load, run_date, filter_col1_name, filter_col1_value) 
    VALUES ('recipe-data', 'N', 'Y', 'N', current_timestamp, 'datePublished', '{0}')"""

sql_update_invoice_pipeline_history = """UPDATE pipeline_history SET is_active = 'N' where id = {0}"""
