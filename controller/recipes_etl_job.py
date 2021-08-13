from dependencies.Spark import Spark
from service import RecipeService as service


def main():
    """Main ETL script definition.
    :return: None
    """
    work_env = "DEV"
    '''
    start Spark application and get Spark session, logger and config
    '''
    spark, log, config = Spark.start_spark(
        app_name='hellofresh_recipes_pipeline', jar_packages=['org.apache.hadoop:hadoop-aws:3.3.1', 'org.postgresql:postgresql:42.2.16'],
        files=['configs/etl_config.json'], environment=work_env)

    '''
    Configure S3 credentials
    '''
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3a.access.key", config['awsAccessKeyId'])
    hadoop_conf.set("fs.s3a.secret.key", config['awsSecretAccessKey'])

    # log that main ETL job is starting
    log.info('hellofresh_recipes_pipeline is up-and-running')

    recipe_service = service.RecipeService(spark, config, log)
    '''
    Task1 : read, pre-process and persist rows to ensure optimal structure and performance for further processing.
    '''
    recipe_service.prepare_and_persist_recipe_data()

    '''
    Task2 : read parquet data and prepare aggregated data
    '''
    recipe_service.read_and_prepare_aggregated_recipe_data()

    # log the success and terminate Spark application
    log.info('hellofresh_recipes_pipeline is finished')
    spark.stop()
    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
