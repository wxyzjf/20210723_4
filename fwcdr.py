from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import Row

from pyspark.sql.types import *

from datetime import datetime

from pyspark import SparkContext,SparkConf


spark = SparkSession \
    .builder \
    .appName("fwcdr_256") \
    .config("spark.executor.memory","7G")\
    .config('spark.executor.instances',50) \
    .config('spark.debug.maxToStringFields',50)\
    .getOrCreate()

print(datetime.now())


mySchema = StructType([
            StructField("rowkey",StringType(), True),
            StructField("ts",StringType(), True),
            StructField("srcip",StringType(), True),
            StructField("dstip",StringType(), True),  
            StructField("add",StringType(), True)
            ])
fwcdr = spark.read.schema(mySchema).csv("/HDS_VOL_HIVE/FWCDR/start_date=20201016/*.gz",sep='|')

fwcdr.createOrReplaceTempView("fwcdr")

df = spark.sql("select int(substr(rowkey,9,3)) as pkey,ts,srcip,dstip,\
                       substring_index(add,'-',1) as send_vol,\
                       substring_index(substring_index(add,'-',2),'-',-1) as recv_vol,\
                       substring_index(add,'-',-1) as srv\
                from fwcdr")\
          .repartition('pkey')
df.write.save("/HDS_VOL_TMP/fwcdr_t",mode='append',compression='gzip',partitionBy='pkey')           

print(datetime.now())
