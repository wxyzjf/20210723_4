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
sch_lqs =StructType([ StructField("part_key",StringType(), True),
					  StructField("start_date",IntegerType(), True),
					  StructField("end_date",IntegerType(), True),
                      StructField("ecgi_cgi", StringType(), True), 
                      StructField("zone_code", StringType(), True), 
                      StructField("landmark_eng", StringType(), True), 
					  StructField("landmark_chi", StringType(), True), 
					  StructField("latitude_longtitude", StringType(), True)
					])
	
df= spark.read.csv("maprfs:///HDS_VOL_TMP/ka/lqs.dat",sep='|',schema=sch_lqs).cache()
df.createOrReplaceTempView("v_lqs")
#df.printSchema

sch_pgw =StructType([ StructField("subr_num", StringType(), True), 
                      StructField("dd_tag", StringType(), True), 
                      StructField("enodeb_id", StringType(), True),
                      StructField("lte_cell_id", StringType(), True),
                      StructField("cgi", StringType(), True),
                      StructField("ttl_vol", DecimalType(), True)
					])
df2= spark.read.csv("/HDS_VOL_TMP/ba/pgw_sum/pgw_sum_split*",sep=',',schema=sch_pgw)
df2.createOrReplaceTempView("v_pgw")
#df2.printSchema

spark.sql("select p.subr_num  \
				,p.dd_tag \
				,p.cgi \
				,l.landmark_eng  \
				,l.landmark_chi \
				,sum(p.ttl_vol)  \
			from v_pgw p  \
			left outer join v_lqs l  \
					on p.cgi= l.ecgi_cgi and 29991231 between l.start_date and l.end_date  \
			group by p.subr_num \
				,p.dd_tag \
				,l.landmark_eng \
				,l.landmark_chi \
				,p.cgi")\
			.repartition(4) \
			.write.save("maprfs:///HDS_VOL_TMP/ka/res",compression='gzip',format="csv",mode='append')










