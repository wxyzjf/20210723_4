
from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import  Row

from pyspark.sql.types import *

from datetime import datetime
from pyspark.sql.functions import *

spark = SparkSession\
    .builder \
    .appName("mall local test") \
    .config("spark.executor.memory","10G")\
    .config('spark.executor.memoryOverhead','5G')\
    .config('spark.executor.instances',15) \
    .config('spark.debug.maxToStringFields',50)\
    .getOrCreate()

#df= spark.read.csv("/HDS_VOL_HIVE/mall_ebm/{0,2}.tar.gz",sep=';',header='True')
df=spark.read.csv("file:///home/mapr/ka/test.txt",sep=';',header='True')
df.createOrReplaceTempView("mallcdr")
   
spark.sql("select time,eci,imsi from mallcdr limit 10").groupBy('imsi').agg(collect_list("time")).show()
