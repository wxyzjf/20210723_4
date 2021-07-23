from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import  Row

from pyspark.sql.types import *

from datetime import datetime



def sortKey(elem):
    return elem[0]

def sortAndDedupliaction(x):
    x = list(x)
    x.sort(key=sortKey)
    
    a = 0
    while a < len(x)-1:
      if x[a][1] == x[a+1][1]:
        del x[a+1]
      else:
        x[a] = Row(time=x[a][0], eci=x[a][1], imsi=x[a][2], timeout=x[a+1][0], eciout=x[a+1][1])
	a += 1
    x[a] = Row(time=x[a][0], eci=x[a][1], imsi=x[a][2], timeout='0', eciout='')
    return x

def basic_df_example(spark):
    
    df= spark.read.csv("/HDS_VOL_HIVE/mall_ebm/{0,2}.tar.gz",sep=';',header='True')
    df.createOrReplaceTempView("mallcdr")
    
    spark.sql("select time,eci,imsi,'0' as timeout, '' as eciout from mallcdr") \
	 .repartition(15,"imsi")	\
	 .sortWithinPartitions("imsi","time","eci") \
         .write.save("maprfs:///HDS_VOL_TMP/test_ka/t2",compression='gzip',format="csv",mode='append')
    return 0

spark = SparkSession \
    .builder \
    .appName("mall test") \
    .config("spark.executor.memory","10G")\
    .config('spark.executor.memoryOverhead','5G')\
    .config('spark.executor.instances',15) \
    .config('spark.debug.maxToStringFields',50)\
    .getOrCreate()

print(datetime.now())
basic_df_example(spark)
print(datetime.now())
