from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import  Row

from pyspark.sql.types import *

from datetime import datetime

def basic_df_example(spark):
    
    mySchema = StructType([
		StructField("pkey",StringType(), True),
		StructField("rowkey",StringType(), True),
		StructField("ts",StringType(), True),
		StructField("srcip",StringType(), True),
		StructField("tagip",StringType(), True),
		StructField("col5",StringType(), True)
		])
    fwcdr = spark.read.schema(mySchema).parquet("/HDS_VOL_TMP/test_par_ba/*.parquet")
    fwcdr.createOrReplaceTempView("fwcdr")
    spark.sql("select min(pkey) from fwcdr").show()


spark = SparkSession \
    .builder \
    .appName("parque test") \
    .config("spark.executor.memory","10G")\
    .config('spark.executor.memoryOverhead','5G')\
    .config('spark.executor.instances',10)\
    .getOrCreate()
print(datetime.now())
basic_df_example(spark)
print(datetime.now())
