from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import  Row

from pyspark.sql.types import *

from datetime import datetime

from pyspark.sql.functions import *



spark = SparkSession \
    .builder \
    .appName("mall test ba") \
    .config("spark.executor.memory","10G")\
    .config('spark.executor.memoryOverhead','5G')\
    .config('spark.executor.instances',30) \
    .config('spark.debug.maxToStringFields',50)\
    .config('spark.executor.cores',2)\
    .config('spark.sql.shuffle.partitions',100)\
    .getOrCreate()


embSchema = StructType([
        StructField("evnetid", StringType(), False),
        StructField("TIME", StringType(), False),
        StructField("TIME_MILLISECOND", StringType(), False),
        StructField("ECI", StringType(), False),
        StructField("IMSI", StringType(), False),
        StructField("IMEISV", StringType(), False),
        StructField("EVENT_RESULT", StringType(), False),
        StructField("MNC", StringType(), False),
        StructField("APN", StringType(), False),

])

df= spark.read.schema(embSchema).csv("/HDS_VOL_HIVE/mall_ebm/mall_ebm*.gz",sep=';',header='True')

#park.read.csv("file:///home/mapr/ka/test.csv",sep=';',header='True')

df.createOrReplaceTempView("mallcdr")

decodeSchema = ArrayType(
    StructType([
        StructField("time", StringType(), False),
        StructField("eci", StringType(), False),
        StructField("timeout", StringType(), False),
        StructField("dwell_time", StringType(), False)
]))


def udf_d(sorted_list):
     
    a = 0
    time_out = 0
    while a < len(sorted_list)-1:
      
      elem = sorted_list[a].split('_',1)
      next_elem = sorted_list[a+1].split('_',1)

      if elem[1] == next_elem[1]:
        time_out = next_elem[0]
        del sorted_list[a+1]
      else:
	if time_out != 0 :
          sorted_list[a] = tuple(elem) + (time_out, float(time_out) - float(elem[0]))
	else:
	  sorted_list[a] = tuple(elem) + (elem[0],0)
	time_out = 0
        a += 1

    elem = sorted_list[a].split('_',1)
    if time_out != 0:
      sorted_list[a] = tuple(elem) + (time_out, float(time_out) - float(elem[0]))
    else: 
      sorted_list[a] = tuple(elem) + (elem[0],0)
    return sorted_list



m_udf=udf(lambda s:udf_d(s), decodeSchema)

df = spark.sql("select time,eci,imsi,time||'_'||eci as str from mallcdr") \
	.groupBy('imsi') \
	.agg(m_udf(sort_array(collect_set("str"),True)).alias("eciList")) \
	.rdd.flatMapValues(lambda x : x) \
        .coalesce(10) \
	.toDF() \
	.select('_1','_2.*') \
	.write.save("maprfs:///HDS_VOL_TMP/test_mall_ba",format='csv',mode='append')

