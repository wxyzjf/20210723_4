from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import Row

from pyspark.sql.types import *

from datetime import datetime

from pyspark import SparkContext,SparkConf


#conf = SparkConf().setAppName("fwcdr_256").setAll([('spark.executor.instances',25),('spark.executor.memory','10G'),('spark.executor.memoryOverhead','2G')])
#conf = SparkConf().setAppName("fwcdr_256").setAll([('spark.executor.memory','240G'),\
#                                                   ('spark.driver.maxResultSize','4G'),\
#                                                   ('spark.driver.memory','240G'),\
#                                                   ('spark.sql.shuffle.partitions',100),\
#                                                   ('spark.default.parallelism',100),\
#                                                   ('spark.eventLog.dir','/app/HDSDATA/SPK_HS_LOG/'),\
#                                                   ('spark.executor.cores','30')\
#                                                        ])

conf = SparkConf().setAppName("fwcdr_256").setAll([('spark.executor.instances',28),\
                                                   ('spark.executor.memory','10G'),\
                                                   ('spark.driver.maxResultSize','4G'),\
                                                   ('spark.driver.memory','240G'),\
                                                   ('spark.default.parallelism',100)\
                                                   ])

sc = SparkContext(conf=conf)
spark = SparkSession(sc)

#sc._jsc.hadoopConfiguration().setInt("dfs.blocksize",1024*1024*256)
#sc._jsc.hadoopConfiguration().setInt("parquet.block.size",1024*1024*256)

print(datetime.now())


mySchema = StructType([
            StructField("rowkey",StringType(), True),
            StructField("ts",StringType(), True),
            StructField("srcip",StringType(), True),
            StructField("dstip",StringType(), True),  
            StructField("add",StringType(), True)
            ])
#fwcdr = spark.read.schema(mySchema).csv("file:///app/HDSDATA/fwcdr2/fwcdr_ldr_20*.gz",sep='|')
#fwcdr = spark.read.schema(mySchema).csv("file:///app/HDSDATA/fwcdr2/fwcdr_*.gz",sep='|')
#fwcdr = spark.read.schema(mySchema).csv("file:///app/HDSINPUT/fwcdr/start_date=20200725/split/*fwcdr_ldr*",sep='|')

#fwcdr = spark.read.schema(mySchema).csv("file:///app/HDSDATA/input/start_date=20200724/split/*fwcdr_ldr*",sep='|')
fwcdr = spark.read.schema(mySchema).csv("maprfs:///HDS_VOL_HIVE/FWCDR/start_date=20200801/*fwcdr_ldr*",sep='|')



fwcdr.createOrReplaceTempView("fwcdr")

df = spark.sql("select int(substr(rowkey,9,3)) as pkey,ts,srcip,dstip,add,\
                       cast(replace(substr(rowkey,1,15),'.','') as long) as srcip_num,\
                       cast(concat(lpad(substring_index(dstip,'.',1),3,'0'),\
                              lpad(substring_index(substring_index(dstip,'.',2),'.',-1),3,'0'),\
                              lpad(substring_index(substring_index(dstip,'.',3),'.',-1),3,'0'),\
                              lpad(substring_index(dstip,'.',-1),3,'0')) as long) as dstip_num\
                from fwcdr")\
          .repartition('pkey')\
          .sortWithinPartitions("pkey","srcip_num") \
          .write.save("maprfs:///HDS_VOL_TMP/el_oneday/bigdataetl02/fwcdr/start_date=20200801",mode='append',compression='gzip',partitionBy='pkey')
           
       # .repartition('pkey','srcip_num')\

#df.printSchema()
#df.show(10)


#df.write.save("maprfs:///HDS_VOL_TMP/el_oneday/csv",format='csv',mode='append',compression='gzip',partitionBy='pkey')
#df.write.save("file:///app/HDSDATA/SPK_OUTPUT/el_oneday/parquet",mode='append',compression='gzip',partitionBy='pkey')


print(datetime.now())













