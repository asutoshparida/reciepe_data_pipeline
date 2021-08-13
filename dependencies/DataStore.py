"""
DataStore.py
~~~~~~~~

Module containing helper function for connecting to postgres
"""
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from miscellaneous import ApplicationConstants as constant


class DataStore(object):
    """Wrapper class for DataStore JVM object.

    :param conf: config object.
    :param log: log object.
    """

    def __init__(self, conf, log):
        # get spark app configs
        self.conf = conf
        self.logger = log

    def create_connection(self):
        """ create a database connection to the postgres database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = psycopg2.connect(
                database=self.conf["db_schema"],
                user=self.conf["db_user"],
                password=self.conf["db_pass"],
                host=self.conf["db_host"],
                port=self.conf["db_port"]
            )
        except errors as e:
            self.logger.error("Error # create_connection :" + e)
            raise e

        return conn

    def execute_query(self, conn, sql_statement):
        """ execute query in database
        :param conn: Connection object
        :param sql_statement: a CREATE TABLE statement
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(sql_statement)
            conn.commit()
        except errors as e:
            self.logger.error("Error # execute_query :" + e)
            raise e
        finally:
            conn.close()

    def select_run_history_by_pipeline(self, conn, etl_name):
        """
        Query pipeline_history by etl_name
        :param conn: the Connection object
        :param etl_name:
        :return: run_history
        """
        run_history = {}
        try:
            cur = conn.cursor()
            select_query = "SELECT id, etl_name, skip_execution, is_active, full_load, filter_col1_name," \
                           "filter_col1_value FROM pipeline_history WHERE is_active  = 'Y' AND etl_name= '{0}';".format(
                            etl_name)
            cur.execute(select_query)

            rows = cur.fetchone()
            run_history = {'id': rows[0], 'etl_name': rows[1], 'skip_execution': rows[2], 'is_active': rows[3],
                           'full_load': rows[4], 'filter_col1_name': rows[5], 'filter_col1_value': rows[6]}
        except errors as e:
            self.logger.error("Error # select_run_history_by_pipeline :" + e)
            raise e
        finally:
            conn.close()

        return run_history

    def insert_recipe_pipeline_history(self, conn, max_publish_date):
        """
        After success run make a history table entry with max publish_date
        :param conn:
        :param max_publish_date:
        :return:
        """
        try:
            insert_query = constant.sql_invoice_pipeline_history.format(max_publish_date)
            self.logger.info("# insert_recipe_pipeline_history for insert_query :" + insert_query)
            self.execute_query(conn, insert_query)
        except errors as e:
            self.logger.error("Error # insert_recipe_pipeline_history :" + e)
            raise e
        finally:
            conn.close()

    def update_recipe_pipeline_history(self, conn, run_id):
        """
        After success run update history table entry with is_active = 'N'
        :param conn:
        :param run_id:
        :return:
        """
        try:
            update_query = constant.sql_update_invoice_pipeline_history.format(run_id)
            self.logger.info("# update_recipe_pipeline_history with update_query :" + update_query)
            self.execute_query(conn, update_query)

        except errors as e:
            self.logger.error("Error # update_recipe_pipeline_history :" + e)
            raise e
        finally:
            conn.close()
