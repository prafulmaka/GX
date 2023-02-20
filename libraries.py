# Import dependencies

from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DatasourceConfig
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.base import FilesystemStoreBackendDefaults
from great_expectations.core.batch import RuntimeBatchRequest
import datetime
import pandas as pd
import great_expectations as ge
# import great_expectations.jupyter_ux
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.exceptions import DataContextError
from great_expectations.data_context.store import validations_store
import os
import json
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType, TimestampType, LongType, FloatType
from pyspark.sql.functions import lit, col, monotonically_increasing_id,count,current_timestamp, when, split, element_at, size, get_json_object, coalesce
from pyspark.sql import *
import traceback
from great_expectations.core import ExpectationConfiguration
from pyspark.sql.functions import col, max as max_
import pyodbc
import time
from urllib.parse import quote
import unittest
import os
import sys
from pyspark.sql import SparkSession
from great_expectations.plugins.expectations.expect_column_values_to_contain_praf import ExpectColumnValuesToContainPraf