"""
test_hf_recipe_ingred_report_etl.py
~~~~~~~~~~~~~~~
This module contains unit tests for the transformation steps of the ETL
job defined in hf_recipe_preprocess_etl.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

import json

from common_utils.spark import start_spark
from jobs.hf_recipe_ingred_report_etl import transform_data


class IngredETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        #self.config = json.loads("""{"steps_per_floor": 21}""")
        self.spark, self.logger, self.config = start_spark()
        self.test_input_data_path = 'tests/test_data/ingredient/input/'
        self.test_exp_data_path = 'tests/test_data/ingredient/expected/'
    
    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .parquet(self.test_input_data_path))

        expected_data = (
            self.spark
            .read
            .csv(self.test_exp_data_path))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        # act
        data_transformed = transform_data(input_data, self.logger)

        cols = len(expected_data.columns)
        rows = expected_data.count()

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed.columns])


if __name__ == '__main__':
    unittest.main()