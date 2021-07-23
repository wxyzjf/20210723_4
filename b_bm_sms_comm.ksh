#!/bin/ksh
#
# $Header$
#
# $Locker$
#
# $Log$

export BILLIMGHOME=${BILLIMGHOME:-/app/BILLIMG}
export SCRIPTDIR=${BILLIMGHOME}/script
scriptname=`basename $0 |  cut -f1 -d'.'`
#*#. $SCRIPTDIR/dw_billimg_master_reload.ksh $scriptname N
. /app/BILLIMG/reload/dw_billimg_master_reload_e.ksh $scriptname N  #*#

jobmsg="BM_SMS_COMM loading"
startup "${jobmsg}"

getmonth 1 trx_mth "%Y-%m-01"

echo "START     : "$(date +%d/%m/%Y" "%H:%M:%S) | tee -a $LOGFILE
runsql ${SQLDIR}/b_bm_sms_comm_01.sql  

rundsjobs BM_SMS_COMM

runsql ${SQLDIR}/b_bm_sms_comm_02.sql ${trx_mth} 

runsql ${SQLDIR}/b_bm_sms_comm_03.sql  


runsql ${SQLDIR}/b_bm_sms_comm_04.sql  

runsql ${SQLDIR}/b_bm_sms_comm_05.sql  

runsql ${SQLDIR}/b_bm_sms_comm_06.sql ${trx_mth} 
runsql ${SQLDIR}/b_bm_sms_comm_07.sql ${trx_mth} 

runsql ${SQLDIR}/b_bm_sms_comm_08.sql ${trx_mth} 

echo "Running bm_sms_comm success"|tee -a $LOGFILE  

echo "FINISH     : "$(date +%d/%m/%Y" "%H:%M:%S) | tee -a $LOGFILE

cleanup "${jobmsg}" 
exit 0



