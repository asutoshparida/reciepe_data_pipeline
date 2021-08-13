"""
Spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

from os import environ, listdir, path
import json
from pyspark.sql import SparkSession
import os
from dependencies import Logging


class Spark(object):
    """Wrapper class for Apache Spark.
    """

    @staticmethod
    def start_spark(app_name='my_spark_app', master='local[*]', jar_packages=[],
                    files=[], spark_config={}, environment='DEV'):
        """Start Spark session, get Spark logger and load config files.

        :param app_name: Name of Spark app.
        :param master: Cluster connection details (defaults to local[*]).
        :param jar_packages: List of Spark JAR package names.
        :param files: List of files to send to Spark cluster (master and
            workers).
        :param spark_config: Dictionary of config key-value pairs.
        :param environment: DEV/PROD env.
        :return: A tuple of references to the Spark session, logger and
            config dict (only if available).
        """

        # detect execution environment
        flag_debug = 'DEBUG' in environ.keys()

        spark_jars_packages = ','.join(list(jar_packages))
        submit_args = "--packages {0} pyspark-shell".format(spark_jars_packages)
        os.environ["PYSPARK_SUBMIT_ARGS"] = submit_args

        if not flag_debug or environment.upper() == 'PROD':
            # get Spark session factory
            spark_builder = (
                SparkSession
                .builder
                .enableHiveSupport()
                .appName(app_name))
        else:
            # get Spark session factory
            spark_builder = (
                SparkSession
                .builder
                .master(master)
                .enableHiveSupport()
                .appName(app_name))

            # create Spark JAR packages string
            spark_builder.config('spark.jars.packages', spark_jars_packages)

            if environment.upper() == 'DEV':
                spark_builder.config("spark.sql.session.timeZone", "America/New_York")
            elif environment.upper() == 'PROD':
                spark_builder.config("spark.sql.session.timeZone", "America/Los_Angeles")

            spark_files = ','.join(list(files))
            spark_builder.config('spark.files', spark_files)

            # add other config params
            for key, val in spark_config.items():
                spark_builder.config(key, val)

        # create session and retrieve Spark logger object
        spark_sess = spark_builder.getOrCreate()
        spark_logger = Logging.Log4j(spark_sess)

        # get config file if sent to cluster with --files
        conf_files_dir = "../configs/"
        config_files = [filename
                        for filename in listdir(conf_files_dir)
                        if filename.endswith('config.json')]

        if config_files:
            path_to_config_file = path.join(conf_files_dir, config_files[0])
            with open(path_to_config_file, 'r') as config_file:
                config_dict = json.load(config_file)
            spark_logger.warn('loaded config from ' + config_files[0])
        else:
            spark_logger.warn('no config file found')
            config_dict = None

        return spark_sess, spark_logger, config_dict
