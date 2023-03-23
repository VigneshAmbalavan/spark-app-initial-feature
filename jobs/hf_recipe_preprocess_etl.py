"""
hf_recipe_preprocess_etl.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
This Python module contains spark etl job to preprocess
hello fresh recipe json files into parquet after standarzing
source data
"""
# pylint: disable=E0401

from pyspark.sql.functions import coalesce,col,lit
from common_utils.spark import start_spark
from common_utils.trans_udf import get_duration_udf

def read_data(input_path,spark):
    """Read data from json file
        :param : input_path.
        :return: Spark DataFrame.
    """
    try:
        logger.info("Reading recipe json data files")
        raw_df = spark.read.json(input_path)
        return raw_df
    except Exception as error:
        logger.info("Failed to Read Input Files")
        logger.exception("Error in read_data function " + str(error))


def transform_data(raw_df,logger):
    """Transfrom raw data frame
        :param : raw_df.
        :return: transformed_df.
    """
    try:
        logger.info("Starting to transform raw df")

        transformed_df = raw_df.withColumn("prepTime", \
                                    coalesce(get_duration_udf(col('prepTime')),lit(0))) \
                               .withColumn("cookTime", \
                                    coalesce(get_duration_udf(col('cookTime')),lit(0)))
        return transformed_df

    except Exception as error:
        logger.info("Failed to transform raw data")
        logger.exception("Error in transform_data function " + str(error))


def write_data(transformed_df, output_path):
    """Transfrom raw data frame
        :param : transformed_df
        :param : output_path
    """

    try:
        logger.info("Starting to write data")
        transformed_df.coalesce(1) \
                      .write \
                      .format('parquet') \
                      .mode("overwrite") \
                      .save(output_path)
    except Exception as error:
        logger.info("Failed to write data")
        logger.exception("Error in write_data function " + str(error))


def main():
    """Main ETL script definition.
    :return: None
    """
    # Initialise spark,logger and config objects
    global logger
    spark, logger, config = start_spark(
        app_name='hf_recipe_preprocess_etl',
        files=['configs/hf_recipe_preprocess_etl_config.json'])

    logger.info('recipe etl preprocess job is up-and-running')

    # execute ETL pipeline
    raw_data = read_data(config.get('input_path'),spark)
    print("raw data count:",raw_data.count())

    logger.info('starting transformation step')
    transformed_data = transform_data(raw_data,logger)
    print("trans data count:",transformed_data.count())
    print("output schema",transformed_data.printSchema())

    write_data(transformed_data,config.get('output_path'))

    # log the success and terminate Spark application
    logger.info('recipe etl preprocess job is finished')
    spark.stop()

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
