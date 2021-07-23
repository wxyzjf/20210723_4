

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("slipt file") \
    .config("spark.executor.memory","10G")\
    .config('spark.executor.memoryOverhead','5G')\
    .config('spark.executor.instances',15) \
    .config('spark.debug.maxToStringFields',50)\
    .getOrCreate()

df= spark.read.csv("/HDS_VOL_HIVE/mall_ebm/4.tar.gz",sep=';',header='True') \
	.repartition(10) \
	.write.save("maprfs:///HDS_VOL_HIVE/mall_ebm/split_files",format='csv',compression='gzip',mode='append')
