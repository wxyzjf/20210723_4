from pyspark.sql import SparkSession
from pyspark.sql import  Row
from pyspark.sql.types import *
from datetime import datetime


spark = SparkSession \
    .builder \
    .appName("mall lqs") \
    .config("spark.executor.memory","10G")\
    .config('spark.executor.memoryOverhead','5G')\
    .config('spark.executor.instances',15) \
    .config('spark.debug.maxToStringFields',50)\
    .getOrCreate()
	

sch_pgw =StructType([ StructField("subr_num", StringType(), True),
                      StructField("dd_tag", StringType(), True),
		      StructField("cgi", StringType(), True),
                      StructField("landmark_eng", StringType(), True),
                      StructField("landmark_chi", StringType(), True),
                      StructField("ttl_vol", DecimalType(), True)
		    ])

df= spark.read.csv("maprfs:///HDS_VOL_TMP/ka/res/part-*-dbde761a-2aca-4507-8143-d6f964a859da-c000.csv.gz", sep=',', schema=sch_pgw)
df.createOrReplaceTempView("v_pgw")

sch_subr =StructType([ StructField("subr_num", StringType(), True) ])

df2= spark.read.csv("maprfs:///HDS_VOL_TMP/ba/bp_subr_num.txt", sep=',',schema=sch_subr).cache()
df2.createOrReplaceTempView("bp_subr_info")


spark.sql("select v.subr_num, v.dd_tag, v.ttl_vol, v.cgi, v.landmark_eng, v.landmark_chi \
	     from v_pgw v \
	     left outer join bp_subr_info s \
	          on v.subr_num = s.subr_num \
	    where s.subr_num is not null ") \
     .repartition(1)\
     .write.save("maprfs:///HDS_VOL_TMP/ba/res", compression='gzip', format='csv',mode='append')










