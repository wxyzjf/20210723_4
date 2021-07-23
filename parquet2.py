from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import  Row

from pyspark.sql.types import *

from datetime import datetime

def basic_df_example(spark):
    
    mySchema = StructType([
		StructField("rowkey",StringType(), True),
		StructField("ts",StringType(), True),
		StructField("srcip",StringType(), True),
		StructField("tagip",StringType(), True),
		StructField("vol",StringType(), True)
		])
#    fwcdr = spark.read.schema(mySchema).csv("/HDS_VOL_HIVE/FWCDR/start_date=20200703/fwcdr_ldr_*_p1_*.gz",sep='|')
    fwcdr = spark.read.schema(mySchema).csv("/HDS_VOL_HIVE/FWCDR/start_date=20200710//fwcdr_ldr_20200710_20200710231310_p1_001.gz",sep='|')
    fwcdr.createOrReplaceTempView("fwcdr")
    spark.sql("select int(substr(rowkey,9,3)/26) as pkey,ts,srcip,tagip,vol from fwcdr")\
	 .repartition(25,["pkey","srcip"])\
	 .sortWithinPartitions("pkey","srcip") \
	 .write.save("maprfs:///HDS_VOL_TMP/test_ka/t2",mode='append',compression='gzip',partitionBy="pkey")

spark = SparkSession \
    .builder \
    .appName("parque test") \
    .config("spark.executor.memory","13G")\
    .config('spark.driver.maxResultSize','4G')\
    .config('spark.default.parallelism',15) \
    .config('spark.executor.instances',25) \
    .config('spark.driver.memory','4G') \
    .config('spark.serializer','org.apache.spark.serializer.KryoSerializer') \
    .getOrCreate()
print(datetime.now())
basic_df_example(spark)
print(datetime.now())
