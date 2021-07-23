from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
spark = SparkSession.builder.appName("sp_miep").getOrCreate()
sc = spark.sparkContext
sch_miep = StructType([ StructField("subr_num", StringType(), True), 
                              StructField("subr_url", StringType(), True), 
                              StructField("uload_size", IntegerType(), True),
                              StructField("dnld_size", IntegerType(), True),
                              StructField("call_dur", IntegerType(), True),
                              StructField("charging_id", StringType(), True),
                              StructField("accs_type_cd", StringType(), True),
                              StructField("accs_point_name", StringType(), True),
                              StructField("sgsn_ip_addr", StringType(), True),
                              StructField("radio_accs_type_cd", StringType(), True),
                              StructField("src_ip_addr", StringType(), True),
                              StructField("imsi", StringType(), True),
                              StructField("accs_date", StringType(), True),
                              StructField("accs_time", StringType(), True),
                              StructField("status", StringType(), True),
                              StructField("user_agent", StringType(), True),
                              StructField("statuscode", StringType(), True),
                              StructField("imei", StringType(), True),
                              StructField("dialleddigit", StringType(), True),
                              StructField("domain", StringType(), True),
                              StructField("content_type", StringType(), True),
                              StructField("part_key", IntegerType(), True),
                            ])
 
df = spark.read.format("csv").option("delimiter","\t").load("maprfs:///HDS_VOL_HIVE/miep/",schema=sch_miep)
df.createGlobalTempView("df_miep")
spark.sql("select subr_num,accs_date,domain,sum(uload_size+dnld_size)as sum_vol,count(*) cnt from global_temp.df_miep group by subr_num,accs_date,domain").write.save("maprfs:///HDS_VOL_TMP/test_miep",foramt='csv',mode='append')
#df.printSchema()
