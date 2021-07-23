from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import  Row

from pyspark.sql.types import *

from datetime import datetime

from pyspark.sql.functions import *



spark = SparkSession \
    .builder \
    .appName("cnss_s11 summary test") \
    .config("spark.executor.memory","10G")\
    .config('spark.executor.instances',40) \
    .config('spark.debug.maxToStringFields',50)\
    .config('spark.executor.cores',2)\
    .config('spark.sql.shuffle.partitions',100)\
    .getOrCreate()

cnssSchema= StructType([
    StructField("sequence", IntegerType(), False),
    StructField("timeperiod", IntegerType(), False),
    StructField("timelength", IntegerType(), False),
    StructField("probeid", IntegerType(), False),
    StructField("service_name", StringType(), False),
    StructField("service_id", IntegerType(), False),
    StructField("bytes_dl", IntegerType(), False),
    StructField("bytes_ul", IntegerType(), False),
    StructField("packets_dl", FloatType(), False),
    StructField("packets_ul", FloatType(), False),
    StructField("active_dt_dl", FloatType(), False),
    StructField("active_dt_ul", FloatType(), False),
    StructField("real_active_dt_dl", FloatType(), False),
    StructField("real_active_dt_ul", FloatType(), False),
    StructField("retransmitted_bytes_dl", FloatType(), False),
    StructField("retransmitted_bytes_ul", FloatType(), False),
    StructField("retransmitted_packets_dl", FloatType(), False),
    StructField("retransmitted_packets_ul", FloatType(), False),
    StructField("out_of_sequence_dl", FloatType(), False),
    StructField("out_of_sequence_ul", FloatType(), False),
    StructField("loss_dl", FloatType(), False),
    StructField("loss_ul", FloatType(), False),
    StructField("client_response_time", FloatType(), False),
    StructField("samples_of_crt", FloatType(), False),
    StructField("server_response_time", FloatType(), False),
    StructField("samples_of_srt", FloatType(), False),
    StructField("application_response_time", FloatType(), False),
    StructField("samples_of_art", FloatType(), False),
    StructField("worst_response_time", IntegerType(), False),
    StructField("max_real_bitrate_dl", IntegerType(), False),
    StructField("max_real_bitrate_ul", IntegerType(), False),
    StructField("min_real_bitrate_dl", IntegerType(), False),
    StructField("min_real_bitrate_ul", IntegerType(), False),
    StructField("real_bytes_dl", IntegerType(), False),
    StructField("real_bytes_ul", IntegerType(), False),
    StructField("service_duration", FloatType(), False),
    StructField("xdr_id", IntegerType(), False),
    StructField("session_xdr_id", IntegerType(), False),
    StructField("imsi", StringType(), False),
    StructField("msisdn", StringType(), False),
    StructField("ue_ip", StringType(), False),
    StructField("imei", StringType(), False),
    StructField("imei_tac_raw", StringType(), False),
    StructField("imei_tac", StringType(), False),
    StructField("ue_manufacturer", StringType(), False),
    StructField("ue_marketing_name", StringType(), False),
    StructField("ue_band", StringType(), False),
    StructField("ue_category", StringType(), False),
    StructField("ue_radio_options", StringType(), False),
    StructField("ue_subgroup", StringType(), False),
    StructField("apn", StringType(), False),
    StructField("home_country", StringType(), False),
    StructField("home_network", StringType(), False),
    StructField("roaming_type", StringType(), False),
    StructField("roaming_country", StringType(), False),
    StructField("roaming_network", StringType(), False),
    StructField("tac", IntegerType(), False),
    StructField("lac_hex", StringType(), False),
    StructField("ci_hex", StringType(), False),
    StructField("sac_hex", StringType(), False),
    StructField("rac_hex", StringType(), False),
    StructField("eci", IntegerType(), False),
    StructField("group_mark", IntegerType(), False),
    StructField("mme_ip", StringType(), False),
    StructField("sgsn_ip", StringType(), False),
    StructField("sgw_ip", StringType(), False),
    StructField("pgw_ip", StringType(), False),
    StructField("epdg_ip", StringType(), False),
    StructField("rat", StringType(), False),
    StructField("cause", StringType(), False),
    StructField("req_apn_ambr_ul", IntegerType(), False),
    StructField("req_apn_ambr_dl", IntegerType(), False),
    StructField("neg_apn_ambr_ul", IntegerType(), False),
    StructField("neg_apn_ambr_dl", IntegerType(), False),
    StructField("warning_apnambr_dl", StringType(), False),
    StructField("warning_apnambr_ul", StringType(), False),
    StructField("enb_u_p_ip", StringType(), False),
    StructField("sgw_u_p_ip", StringType(), False),
    StructField("pgw_u_p_ip", StringType(), False),
    StructField("sgsn_u_p_ip", StringType(), False),
    StructField("epdg_u_p_ip", StringType(), False),
    StructField("rnc_u_p_ip", StringType(), False),
    StructField("s12_gdt", StringType(), False),
    StructField("procedure_type", StringType(), False),
    StructField("eci_hex", StringType(), False),
    StructField("procedure_type_code", StringType(), False),
    StructField("tac_hex", StringType(), False),
    StructField("mme_ip_raw", StringType(), False),
    StructField("sgsn_ip_raw", StringType(), False),
    StructField("sgw_ip_raw", StringType(), False),
    StructField("pgw_ip_raw", StringType(), False),
    StructField("epdg_ip_raw", StringType(), False),
    StructField("enb_u_p_ip_raw", StringType(), False),
    StructField("sgw_u_p_ip_raw", StringType(), False),
    StructField("pgw_u_p_ip_raw", StringType(), False),
    StructField("sgsn_u_p_ip_raw", StringType(), False),
    StructField("epdg_u_p_ip_raw", StringType(), False),
    StructField("rnc_u_p_ip_raw", StringType(), False),
])

cnss_s11 = spark.read.schema(cnssSchema).csv("/HDS_VOL_TMP/cnss/*.gz",sep=',')

cnss_s11.createOrReplaceTempView("cnss_s11")

cnss_rs = spark.sql("""
	select sum(real_bytes_dl)/1000000 as dl_vol
	     , sum(real_bytes_ul)/1000000 as ul_vol
	     , msisdn
	     , from_unixtime(timeperiod,'yyyyMMddHH')  as time
	     , replace(format_number(avg(client_response_time / if(samples_of_crt=0,1,samples_of_crt) * 1000),0),',','') as client_response_time
             , replace(format_number(avg(server_response_time / if(samples_of_srt=0,1,samples_of_srt) * 1000),0),',','') as server_response_time
             , replace(format_number(avg(application_response_time / if(samples_of_art=0,1,samples_of_art) * 1000),0),',','') as application_response_time
	     , avg(worst_response_time)*1000 as worst_response_time
	     , max(max_real_bitrate_dl)*8*1000 as max_real_bitrate_dl
             , max(max_real_bitrate_ul)*8*1000 as max_real_bitrate_ul
	  from cnss_s11
	 group by from_unixtime(timeperiod,'yyyyMMddHH'), service_name, msisdn""")

cnss_rs.write.save("/HDS_VOL_TMP/ba/cnss_rs",mode='append',compression='gzip',format='csv')

