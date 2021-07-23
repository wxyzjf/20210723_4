
from pyspark.sql import SparkSession
from pyspark.sql import  Row
from pyspark.sql.types import *
from datetime import datetime


spark = SparkSession \
    .builder \
    .appName("smcin file split") \
    .config("spark.executor.memory","10G")\
    .config('spark.executor.memoryOverhead','5G')\
    .config('spark.executor.instances',15) \
    .config('spark.debug.maxToStringFields',50)\
    .getOrCreate()



df2 = spark.read.csv("maprfs:///HDS_VOL_TMP/smcin/part_key=20200720/SMCIN_20200720_20200720145019_01_proc_1.gz", sep='\t')\
	.repartition(3) \
        .write.save("maprfs:///HDS_VOL_TMP/smcin_split", compression='gzip', format='csv',mode='append')
