from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

sc = spark.sparkContext
sch_miep = StructType([
        StructField("msisdn",StringType(),True)
        ,StructField("service_name",StringType(),True)
        ,StructField("timeperiod",LongType(),True)
        ,StructField("part_key",IntegerType(),True)
])
myDomainSchema = StructType([ StructField("subr_num", StringType(), True), 
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
 
df = spark.read.text('maprfs://HDS_VOL_HIVE/miep/',schema=sch_miep,delimiter='|')
df.printSchema()
spark.quit()
