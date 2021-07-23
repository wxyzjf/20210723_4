from pyspark.sql.functions import *


#df= spark.read.csv("/HDS_VOL_HIVE/mall_ebm/{0,2}.tar.gz",sep=';',header='True')


df=spark.read.csv("file:///home/mapr/ka/test.csv",sep=';',header='True')
df.createOrReplaceTempView("mallcdr")
   
def udf_d(sorted_list):
	res_list=list()
	for f in sorted_list:
		res_list.append(f+"<<<<")
	return res_list

m_udf=udf(lambda s:udf_d(s),StringType())
spark.sql("select time,eci,imsi,time||'_'||eci as str from mallcdr limit 10").groupBy('imsi').agg(m_udf(sort_array(collect_list("str"),True))).show(1000,False)
