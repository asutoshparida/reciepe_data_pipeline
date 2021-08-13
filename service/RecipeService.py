"""
RecipeService.py
~~~~~~~~

Module containing service function for specific pipelines
"""

from helpers import GenericHelper as helper
from dependencies import DataStore as db

class RecipeService(object):
    """service class for recipe pipeline

    :param conf: config object.
    :param log: log object.
    """

    def __init__(self, spark, conf, log):
        # get log app configs
        self.spark = spark
        self.config = conf
        self.logger = log

    def prepare_and_persist_recipe_data(self):
        '''
        Task 1 :
        Read recipe data incrementally from source then persist it as parquet after data cleansing
        Here I am assuming that data in source are loaded date wise.
        :param self:
        :return:
        '''
        final_recipe_df = None
        generic_helper = helper.GenericHelper(self.config, self.logger)

        run_history = self.prepare_run_data_from_pipeline_history()

        run_id = run_history['id']
        full_load = run_history['full_load'] if run_history is not None else 'Y'
        skip_execution = run_history['skip_execution'] if run_history is not None else 'Y'
        filter_col1_name = run_history['filter_col1_name'] if run_history is not None else 'datePublished'
        filter_col1_value = run_history['filter_col1_value'] if run_history['filter_col1_value'] is not None and \
                                                                run_history['filter_col1_value'] != 'null' else None
        self.logger.info('pipeline running with : run_id :' + str(
            run_id) + "# full_load :" + full_load + "# skip_execution:" + skip_execution + "# filter_col1_name:" + filter_col1_name + "# filter_col1_value :" + str(
            filter_col1_value))

        if run_id is not None and skip_execution == 'N':

            if full_load == 'Y':
                '''
                    If it's a full load then dump the data as parquet with SaveMode: overwrite
                '''
                self.logger.info('prepare_and_persist_recipe_data: Running full load ')
                final_recipe_df = generic_helper.load_cleanse_recipes_data(self.spark, self.config, None)
                generic_helper.export_recipe_data_as_parquet(final_recipe_df, self.config, "overwrite")
            elif full_load == 'N' and filter_col1_value is not None:
                '''
                    If it's a delta load then dump the data as parquet with SaveMode: append
                '''
                self.logger.info('prepare_and_persist_recipe_data: Running delta load with filter_col1_value :' + str(filter_col1_value))
                final_recipe_df = generic_helper.load_cleanse_recipes_data(self.spark, self.config, filter_col1_value)
                generic_helper.export_recipe_data_as_parquet(final_recipe_df, self.config, "append")
            else:
                self.logger.error(
                    'ERROR : Some thing wrong with processing pipeline as filter_col1_value is None. '
                    'Stopping pipeline execution.')

            '''
                retrieve max_publish_date for that run then makes a new pipeline_history 
                and update the current pipeline_history entry is_active to 'N'
            '''
            self.update_run_data_to_pipeline_history(run_id, final_recipe_df, generic_helper)

        else:
            self.logger.warn(
                'WARN : Some thing wrong with processing pipeline as run_id is None OR skip_execution = Y '
                'Stopping pipeline execution.')


    def read_and_prepare_aggregated_recipe_data(self):
        '''
        Task 2 :
        Read recipe data parquet file and prepare data for
        > Extract only recipes that have beef as one of the ingredients.
        > Calculate average cooking time duration per difficulty level.
        :param self:
        :return:
        '''
        generic_helper = helper.GenericHelper(self.config, self.logger)
        recipe_data_df = generic_helper.import_parquet_as_data_frame(self.spark, self.config)
        if recipe_data_df is not None:
            self.logger.info('read and prepare recipe data that have beef as one of the ingredients:')
            final_data_frame = generic_helper.prepare_recipes_data_for_aggregation(recipe_data_df)
            self.logger.info('prepare and write avg time by difficulty level as a csv file:')
            generic_helper.prepare_write_data_for_recipe_difficulty_level(final_data_frame)


    def prepare_run_data_from_pipeline_history(self):
        '''
        Connect to postgres & get pipeline run history data
        :param self:
        :return: run_history
        '''

        postgres_store = db.DataStore(self.config, self.logger)
        conn = postgres_store.create_connection()
        run_history = postgres_store.select_run_history_by_pipeline(conn, 'recipe-data')

        return run_history

    def update_run_data_to_pipeline_history(self, run_id, final_recipe_df, generic_helper):
        '''
        Connect to postgres & update pipeline_history data after each data load
        :param run_id:
        :param final_recipe_df:
        :param generic_helper:
        :return:
        '''

        max_publish_date = generic_helper.select_max_publish_date(final_recipe_df)
        self.logger.info('update_run_data_to_pipeline_history: updating with max_publish_date :' + str(max_publish_date))
        postgres_store = db.DataStore(self.config, self.logger)

        conn = postgres_store.create_connection()
        postgres_store.update_recipe_pipeline_history(conn, run_id)

        conn = postgres_store.create_connection()
        postgres_store.insert_recipe_pipeline_history(conn, max_publish_date)

