# recipes_pipeline

This repository will be used for managing all the code related to pipeline for recipes data.

## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |-- dependencies/
 |   |-- DataStore.py
 |   |-- Logging.py
 |   |-- Spark.py
 |-- helpers/
 |   |-- CustomUDF.py
 |   |-- GenericHelper.py
 |-- controller/
 |   |-- recipe_etl_job.py
 |-- miscellaneous  /
 |   |-- ApplicationConstants.py
 |-- script  /
 |   |-- lambda
         |-- personio_pipeline_lambda.py
 |-- input  /
     |-- recipes-000.json /
     |-- recipes-001.json /
     |-- recipes-002.json /
 |-- output  /
     |-- part-00000 /
 |-- processed-data  /
     |-- recipe.parquet /
 |-- tests/
 |   |-- test_data/
 |   |-- | -- test_data/
 |   |-- test_recipes_etl_job.py
 |   build_dependencies.sh
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
```

# Requirements

    python 3.6 or higher
    Spark 2.3.3
    postgreSQL 10.X
    *AWS Lambda
    *AWS EMR

    * If we want to run/schedule the job on AWS EMR Using AWS Lambda.
    
# postgreSQL schema & table Creation

    CREATE DATABASE hellofresh;
    
    CREATE TABLE IF NOT EXISTS pipeline_history (
       id SERIAL,
       etl_name CHAR(60) NOT NULL,
       skip_execution CHAR(1) NOT NULL,
       is_active CHAR(1) NOT NULL,
       full_load CHAR(1) NOT NULL,
       run_date timestamp,
       filter_col1_name VARCHAR(100),
       filter_col1_value  VARCHAR(1000),
       filter_col2_name VARCHAR(100),
       filter_col2_value VARCHAR(1000)
       );

    INSERT INTO pipeline_history (etl_name , skip_execution, is_active, full_load, run_date, filter_col1_name)
    VALUES ('recipe-data', 'N', 'Y', 'Y', current_timestamp, 'datePublished');
   
# Build & Run Local-Machine
    
    1. Change your configs/etl_config.json entries according to your 
       configuration.
       
    2. Go to package root folder & run
        > ./build_dependencies.sh
    
    3. Then run controller/recipes_etl_job.py mode.
    
# Run On AWS EMR

    1. Configure script/lambda/recipe_lambda.py with propper
        
        "spark.driver.cores": "2",
        "spark.executor.cores": "2",
        "spark.driver.memory": "13G",
        "spark.executor.memory": "13G",
        "spark.driver.memoryOverhead": "1460M",
        "spark.executor.memoryOverhead": "1460M",
        "spark.executor.instances": "10",
        "spark.default.parallelism": "50"
        
    2. Create a new AWS Lambda script taking the script from hellofresh_recipe_lambda.py.(If we need to schedule this job we can use cloud watch events)
    
    3. Then copy the whole code base to s3://bucket/key/ location. Update the same path in lambda
    
    4. Launch the cluster through lambda.
    
    
