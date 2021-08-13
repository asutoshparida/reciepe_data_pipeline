"""
GenericHelper.py
~~~~~~~~

Module containing generic helpers for our pipeline
"""
from pyspark.sql.functions import col, lit, split, to_date, udf, year, month, trim, when, round, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType
from .CustomUDF import CustomUDF


class GenericHelper(object):
    """class containing all generic functions

    :param conf: config object.
    :param log: log object.
    """

    raw_recipe_schema = StructType(fields=[
        StructField('name', StringType(), True),
        StructField('ingredients', StringType(), True),
        StructField('datePublished', StringType(), True),
        StructField('cookTime', StringType(), True),
        StructField('prepTime', StringType(), True),
        StructField('description', StringType(), True),
        StructField('image', StringType(), True),
        StructField('recipeYield', StringType(), True),
        StructField('url', StringType(), True)
    ])

    parquet_recipe_schema = StructType(fields=[
        StructField('recipeName', StringType(), True),
        StructField('datePublished', DateType(), True),
        StructField('cookTimeMinutes', IntegerType(), True),
        StructField('prepTimeMinutes', IntegerType(), True),
        StructField('description', StringType(), True),
        StructField('image', StringType(), True),
        StructField('recipeYield', StringType(), True),
        StructField('url', StringType(), True),
        StructField('ingredients', ArrayType(StringType(), True), True),
        StructField('cookTimeMinutes', IntegerType(), True),
        StructField('prepTimeMinutes', IntegerType(), True),
    ])

    def __init__(self, conf, log):
        # get log app configs
        self.conf = conf
        self.logger = log

    def load_cleanse_recipes_data(self, spark, config, max_publish_date):
        """read the json files and prepare data frame after data quality checks
        :return: DataFrame

                    root
                     |-- recipeName: string (nullable = true)
                     |-- datePublished: date (nullable = true)
                     |-- cookTimeMinutes: integer (nullable = true)
                     |-- prepTimeMinutes: integer (nullable = true)
                     |-- description: string (nullable = true)
                     |-- image: string (nullable = true)
                     |-- recipeYield: string (nullable = true)
                     |-- url: string (nullable = true)
                     |-- ingredients_arr: array (nullable = true)
                     |    |-- element: string (containsNull = true)
                     |-- year: integer (nullable = true)
                     |-- month: integer (nullable = true)
        """
        formatted_recipes_df = None
        report_file_path = config['input_data_path']

        get_minutes = udf(lambda z: CustomUDF().convert_string_minutes(z), IntegerType())

        '''
        Convert cookTime and prepTime strings to minutes using UDF. 
        Convert ingredients string to array after splitting by \n.
        Convert datePublished string to date object.
        '''
        raw_recipes_df = spark.read.json(report_file_path, schema=self.raw_recipe_schema) \
            .select(trim(col("name")).alias("recipeName"),
                    to_date(col("datePublished"), "yyyy-MM-dd").alias("datePublished"),
                    get_minutes(col("cookTime")).alias("cookTimeMinutes"), col("cookTime"),
                    get_minutes(col("prepTime")).
                    alias("prepTimeMinutes"), col("prepTime"), col("description"),
                    col("image"), col("recipeYield"), col("url"), split(col("ingredients"), "\n")
                    .alias("ingredients_arr")) \
            .drop("ingredients", "cookTime", "prepTime", "name")

        '''
        Add new columns year and month from datePublished to partition the data
        after doing a filter on name column for null values.
        '''
        tmp_formatted_recipes_df = raw_recipes_df.na.drop(subset=["recipeName"]).withColumn("year",
                                                                                            year(col(
                                                                                                "datePublished")).cast(
                                                                                                "Integer")) \
            .withColumn("month", month(col("datePublished")).cast("Integer"))

        '''logic for selecting only delta data'''
        if max_publish_date is not None:
            formatted_recipes_df = tmp_formatted_recipes_df.select("*").where(
                tmp_formatted_recipes_df['datePublished'] > lit(max_publish_date))
        else:
            formatted_recipes_df = tmp_formatted_recipes_df

        # formatted_recipes_df.printSchema()

        # formatted_recipes_df.show(60, truncate=False)

        return formatted_recipes_df

    def export_recipe_data_as_parquet(self, data_frame, config, write_mode):
        """
        Takes a DataFrame & bulk insert as parquet file
        :param data_frame:
        :param config:
        :param write_mode:
        :return:
        """
        self.logger.info(
            "Exporting data as parquet to path:" + config['parquet_file_location'] + ", with write mode :" + write_mode)
        try:

            parquet_file_path = config['parquet_file_location']

            data_frame.repartition('year') \
                .write \
                .mode(write_mode) \
                .partitionBy('year', 'month') \
                .option("schema", self.parquet_recipe_schema) \
                .parquet(parquet_file_path)

        except Exception as e:
            self.logger.error("Error # export_data_frame_as_parquet :", e)
            raise e

    def import_parquet_as_data_frame(self, spark, config):
        """
        Read parquet file and dump data to dataFrame
        :param spark:
        :param config:
        :return: parqet_data_df
        """
        self.logger.info(
            "Importing data from parquet:" + config['parquet_file_location'])
        try:

            report_file_path = config['parquet_file_location']

            parquet_data_df = spark.read.parquet(report_file_path)
            # parquet_data_df.show(120, truncate=False)
        except Exception as e:
            self.logger.error("Error # import_parquet_as_data_frame :", e)
            raise e

        return parquet_data_df

    def select_max_publish_date(self, recipe_data_frame):
        """
        extract maximum datePublished for that run
        :param recipe_data_frame:
        :return: max_invoice_id
        """
        max_publish_date = recipe_data_frame.agg({"datePublished": "max"}).collect()[0][0]
        self.logger.info("# select_max_publish_date :" + str(max_publish_date))

        return str(max_publish_date)

    def prepare_recipes_data_for_aggregation(self, data_frame):
        """read the parquet files and prepare data for reports
        :return: DataFrame

                    root
                     |-- recipeName: string (nullable = true)
                     |-- is_beef_recipe: integer (nullable = true)
                     |-- total_time: integer (nullable = true)
                     |-- difficulty: string (nullable = false)
        """

        arr_contains = udf(lambda x, y: CustomUDF().check_array_contains_str(x, y), IntegerType())
        tmp_data_frame = data_frame.select(col("recipeName"),
                                           when(col("ingredients_arr").isNull(), 0).otherwise(
                                               arr_contains(col("ingredients_arr"), lit("beef"))).
                                           alias("is_beef_recipe"),
                                           (col("cookTimeMinutes") + col("prepTimeMinutes")).alias("total_time"))

        final_data_frame = tmp_data_frame.withColumn("difficulty", when(col("total_time") < 30, "easy")
                                                     .when(col("total_time").between(30, 60), "medium").otherwise(
            "hard"))

        final_data_frame.printSchema()

        recipe_with_beef_df = final_data_frame.select(col("recipeName")).where(col("is_beef_recipe") == 1)
        self.logger.info("### recipes that have beef as one of the ingredients ###")
        recipe_with_beef_df.show(60, truncate=False)

        return final_data_frame

    def prepare_write_data_for_recipe_difficulty_level(self, data_frame):
        '''
        Write data to csv for recipe difficulty level Avg time
        :param data_frame:
        :return:
        '''
        if data_frame is not None:
            difficulty_df = data_frame.groupBy("difficulty").agg(round(avg("total_time")).alias("avg_total_cooking_time")).coalesce(1)
            difficulty_df.write.option("header", True).mode("overwrite").csv(self.conf['output_data_path'])
