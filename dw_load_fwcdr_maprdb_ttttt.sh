export DWHOME=${DWHOME:-/opt/reload}
export SCRIPTDIR=${DWHOME}/script

scriptname=`basename $0 |  cut -f1 -d'.'`
. /opt/reload/test_e/dw_master_hds_reload.sh $scriptname N

jobmsg="Start $scriptname Loading"

#startup "${jobmsg}"

optdate=`date +"%Y%m%d"`
filedate=`date -d "$optdate-2 day" +%Y%m%d`
clsdate=`date -d "$optdate-10 day" +%Y%m%d`

mon=`date +"%m"`
 
if [ -f /opt/reload/test_e/input/FWCDR/dw_fw_cdr_${optdate}.complete ];then
  cat /dev/null >${TMPDIR}/dw_load_fwcdr.sql
  echo "alter table h_fw_cdr_test add if not exists partition (start_date='$filedate'); " >>${TMPDIR}/dw_load_fwcdr.sql | tee -a ${LOGFILE}
  echo "alter table h_fw_cdr_test drop partition (start_date='$clsdate')" >>${TMPDIR}/dw_load_fwcdr.sql | tee -a ${LOGFILE}
  runhivesql ${TMPDIR}/dw_load_fwcdr.sql | tee -a ${LOGFILE}
else
  echo "Source data not ready from bigdataetl01 " | tee -a ${LOGFILE}
  errhandle "Source data not ready from bigdataetl01 "
  exit 99
fi


echo "Start to load Hbase at `date`" | tee -a ${LOGFILE}
##java -cp "`hbase classpath`" org.apache.hadoop.hbase.mapreduce.ImportTsv -Dmapreduce.tasktracker.map.tasks.maximum=30 '-Dimporttsv.separator=|'  -Dcreate.table=no -Dimporttsv.skip.bad.lines=false -Dimporttsv.columns="HBASE_ROW_KEY,cf:start_datetime,cf:src,cf:json_value" /HDS_VOL_HBASE/FW_CDR_${mon} maprfs:///HDS_VOL_HIVE/FWCDR/start_date=${filedate}/fwcdr_ldr*gz 2>&1 | tee -a ${LOGFILE}
#
#
java -cp "`hbase classpath`" org.apache.hadoop.hbase.mapreduce.ImportTsv -Dmapreduce.tasktracker.map.tasks.maximum=30 '-Dimporttsv.separator=|'  -Dcreate.table=no -Dimporttsv.skip.bad.lines=false -Dimporttsv.columns="HBASE_ROW_KEY,cf:start_datetime,cf:src,cf:send_volume,cf:receive_volume,cf:service,cf:dst" /HDS_VOL_HBASE/TEST/FW_CDR_${mon} maprfs:///HDS_VOL_HIVE/TEST/FWCDR/start_date=${filedate}/fwcdr_ldr*gz 
#2>&1 | tee -a ${LOGFILE}

cleanup ${jobmsg}
exit 0
