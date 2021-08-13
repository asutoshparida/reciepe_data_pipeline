"""
test_recipes_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in recipes_etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest
from helpers import GenericHelper as helper
from dependencies.Spark import Spark


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in recipes_etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        work_env = "DEV"
        self.spark, self.log, self.config = Spark.start_spark(app_name='hellofresh_recipes_test_pipeline',
                                                              jar_packages=['org.apache.hadoop:hadoop-aws:3.3.1',
                                                                            'org.postgresql:postgresql:42.2.16'],
                                                              files=['../configs/etl_config.json'],
                                                              environment=work_env)
        # assemble
        self.config['input_data_path'] = "../tests/test_data/"
        # act
        self.generic_helper = helper.GenericHelper(self.config, self.log)
        self.data_transformed = self.generic_helper.load_cleanse_recipes_data(self.spark, self.config, None)

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_load_cleanse_recipes_data(self):
        """Test load_cleanse_recipes_data.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assert
        self.assertEqual(484, self.data_transformed.count())

    def test_select_max_publish_date(self):
        """Test select_max_publish_date.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # act
        max_date_str = self.generic_helper.select_max_publish_date(self.data_transformed)
        # assert
        self.assertEqual("2013-03-28", max_date_str)

    def test_prepare_recipes_data_for_aggregation(self):
        """Test prepare_recipes_data_for_aggregation.

                Using small chunks of input data and expected output data, we
                test the transformation step to make sure it's working as
                expected.
                """
        # act
        formatted_df = self.generic_helper.prepare_recipes_data_for_aggregation(self.data_transformed)
        # assert
        self.assertIsNotNone(formatted_df)


if __name__ == '__main__':
    unittest.main()
