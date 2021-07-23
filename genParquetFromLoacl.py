

from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import  Row

from pyspark.sql.types import *

from datetime import datetime




spark = SparkSession \
    .builder \
    .appName("generate parquet file") \
    .config("spark.executor.memory","3G")\
    .config('spark.executor.memoryOverhead','1G')\
    .config('spark.executor.instances',1)\
    .getOrCreate()


df = spark.read.csv('file:///home/mapr/ba/part0000.csv',sep=',')
df.createOrReplaceTempView('fwcdr')

spark.sql('select int(_c0),_c1,_c2 from fwcdr limit 10').\
	write.save('file:///home/mapr/ba/parquet-sample')






