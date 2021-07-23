from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import  Row

from pyspark.sql.types import *

from datetime import datetime

spark = SparkSession \
    .builder \
    .appName("parque test") \
    .config("spark.executor.memory","13G")\
    .config('spark.driver.maxResultSize','4G')\
    .config('spark.default.parallelism',15) \
    .config('spark.executor.instances',25) \
    .config('spark.driver.memory','4G') \
    .getOrCreate()
    
rdd = spark.read.parquet("maprfs:///HDS_VOL_TMP/test_ka/pkey=1/*parquet")
rdd.filter("srcip='1.11.3.244'").show()
