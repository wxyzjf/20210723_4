from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import  Row

from pyspark.sql.types import *

from datetime import datetime

def basic_df_example(spark):
    mySchema = StructType([
		StructField("rowkey",StringType(), True),
		StructField("ts",StringType(), True),
		StructField("srcip",StringType(), True),
		StructField("tagip",StringType(), True),
		StructField("col5",StringType(), True)
		])
    fwcdr = spark.read.schema(mySchema).csv("/HDS_VOL_HIVE/FWCDR/start_date=20200703/*.gz",sep='|')
    fwcdr.createOrReplaceTempView("fwcdr")
    spark.sql("select substr(rowkey,1,7) as partitionKey,count(*)  from fwcdr group by substr(rowkey,1,7)")\
	 .write.save("maprfs:///HDS_VOL_TMP/ka",mode='append',format='csv',compression='gzip')
	# .repartition(26,"partitionKey")\
	# .sortWithinPartitions("partitionKey")\

    


spark = SparkSession \
    .builder \
    .appName("parque test") \
    .config("spark.executor.memory","6G")\
    .config('spark.executor.memoryOverhead','6G')\
    .config('spark.driver.maxResultSize','4G')\
    .getOrCreate()

   # .config('spark.executor.instances',10)\


print(datetime.now())
basic_df_example(spark)
print(datetime.now())
