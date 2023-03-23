"""
trans_udf.py
~~~~~~~

This module contains udf function required for ETL transformation steps for jobs
"""
# pylint: disable=E0401,R0205,R1710,W0108

import isodate
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

def _get_duration(col):
    """Converts ISO time duration format to standard time duration.

        :param: ISO time column
        :return: time in duration
    """
    if col:
        duration = isodate.parse_duration(col)
        duration = (duration.total_seconds()/60)
        return duration

get_duration_udf = udf(lambda z: _get_duration(z), FloatType())
