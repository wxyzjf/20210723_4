from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row


spark = SparkSession.builder.appName("Py_test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
sc=spark.sparkContext

rdd = sc.textFile("hdfs:////HDS_VOL_TMP/adaptor_5g_by_day_20200*/*",minPartitions=10,use_unicode=False).map(lambda x:x.split(",")).repartition(10).map(lambda x:(x[0],x[1],int(x[2]),int(x[3])))
sch=StructType([        
	StructField("msisdn",StringType(),True)
	,StructField("service_name",StringType(),True)
	,StructField("timeperiod",LongType(),True)
	,StructField("part_key",IntegerType(),True)
])
df_srcFile=spark.createDataFrame(rdd,sch)

df_srcFile.write.save("hdfs:///HDS_VOL_HIVE/pqtest2",foramt='parquet',mode='append',compression='gzip')

spark.stop()
