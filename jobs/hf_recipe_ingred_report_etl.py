"""
hf_recipe_ingred_report_etl.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
This Python module contains spark etl job to read preprecessed recipe data
and performs etl calculation of ingredient averaga cooking time,
 and generates csv report
"""
# pylint: disable=E0401,W0703,R1710,C0301

from common_utils.spark import start_spark
from pyspark.sql.functions import col,lower,expr,avg,round

def read_data(input_path, spark):
    """Read data from json file
        :param : input_path.
        :return: Spark DataFrame.
    """
    try:
        logger.info("Reading recipe preprocessed data")
        recipe_df = spark.read.parquet(input_path)
        return recipe_df
    except Exception as error:
        logger.info("Failed to Read Input Files")
        logger.exception("Error in read_data function " + str(error))


def transform_data(recipe_df, logger):
    """Transfrom raw data frame
        :param : raw_df.
        :return: transformed_df.
    """
    try:
        logger.info("Starting to transform recipe df")
        filtered_recipe_df = recipe_df.filter(lower(col('ingredients')).contains('beef'))
        recipe_tot_cooktime_df = filtered_recipe_df \
                                    .withColumn("totalCookTime", \
                                         col('prepTime') + col('cookTime'))
        recipe_diff_lvl_df = recipe_tot_cooktime_df \
                                .withColumn("difficultyLevel", \
                                    expr("CASE WHEN totalCookTime <= 30 THEN 'easy' " +
                                        "WHEN (totalCookTime > 30 and totalCookTime <= 60) THEN 'medium' " +
                                        "WHEN (totalCookTime > 60) THEN 'hard' " +
                                        "WHEN totalCookTime IS NULL THEN ''" +
                                        "ELSE 'unknown' END"))
        avg_cooktime_df = recipe_diff_lvl_df \
                                    .groupBy('difficultyLevel') \
                                    .agg(round(avg('totalCookTime'),2).alias('avgCookTime'))
        return avg_cooktime_df

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
                      .format('csv') \
                      .option("header", "true") \
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
        app_name='hf_recipe_ingred_report_etl',
        files=['configs/hf_recipe_ingred_report_etl_config.json'])

    logger.info('ingred report etl preprocess job is up-and-running')

    # execute ETL pipeline
    recipe_data = read_data(config.get('input_path'), spark)
    print(recipe_data.count())

    calculated_data = transform_data(recipe_data, logger)
    calculated_data.count()

    write_data(calculated_data,config.get('output_path'))
    print(calculated_data.show())

    # log the success and terminate Spark application
    logger.info('ingredient report job is finished')
    spark.stop()

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
