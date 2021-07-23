from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark import SparkContext,SparkConf

from pyspark.sql import  Row

from pyspark.sql.types import *

from pyspark.sql.functions import *

from datetime import datetime

import math

    
conf = SparkConf().setAppName("Test Repartition App").setAll([('spark.executor.instances',13),('soark.executor.memory','3G'),('spark.executor.memoryOverhead','3G')])
sc = SparkContext(conf=conf)

def partitionF(key):
    return key

print(datetime.now())
rdd = sc.textFile("/HDS_VOL_HIVE/FWCDR/start_date=20200627/*gz",use_unicode=False)
fwcdr = rdd.map(lambda line : (int(line[8:11]),line))\
        .repartitionAndSortWithinPartitions(255,lambda k : k%255, True)
print(fwcdr.getNumPartitions())

spark = SparkSession(sc)
df = spark.createDataFrame(fwcdr)

df.write.save("maprfs:///HDS_VOL_TMP/test_par_ba",mode='overwrite',compression='gzip')
print(datetime.now())
