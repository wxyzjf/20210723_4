from pyspark.sql import SparkSession
from pyspark.sql import  Row
from pyspark.sql.types import *
from datetime import datetime


spark = SparkSession \
    .builder \
    .appName("mall lqs") \
    .config("spark.executor.memory","8G")\
    .config('spark.executor.memoryOverhead','4G')\
    .config('spark.driver.memory','4G')\
    .config('spark.executor.instances',15) \
    .config('spark.debug.maxToStringFields',50)\
    .getOrCreate()


df= spark.read.csv("maprfs:///HDS_VOL_TMP/ba/unload_bp_s_reward_subr_list_SHK_Mall.dat.20200720",header=True, sep='|').cache()
df.createOrReplaceTempView("subr_info")


df2 = spark.read.csv("maprfs:///HDS_VOL_TMP/smcin/part_key=20200720/SMCIN*.gz", sep='\t')
df2.createOrReplaceTempView("smcin")



spark.sql("select /*+ BORADCAST(subr) */ smc.* from subr_info subr inner join smcin smc on subr.Subr_Num = smc._c0")\
	.repartition(1) \
	.write.save("maprfs:///HDS_VOL_TMP/ba/smcin", compression='gzip', format='csv',mode='append')


