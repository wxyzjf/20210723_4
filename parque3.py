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
		StructField("col5",StringType(), True)
		])
    fwcdr = spark.read.schema(mySchema).csv("/HDS_VOL_HIVE/FWCDR/start_date=20200703/*.gz",sep='|')
    fwcdr.createOrReplaceTempView("fwcdr")
    spark.sql("select substr(rowkey,9,2) as partitionKey,count(1) from fwcdr group by substr(rowkey,9,2)").show(26)
    


spark = SparkSession \
    .builder \
    .appName("parque test") \
    .config("spark.executor.memory","4G")\
    .config('spark.executor.memoryOverhead','5G')\
    .config('spark.executor.instances',12)\
    .getOrCreate()
print(datetime.now())
basic_df_example(spark)
print(datetime.now())
