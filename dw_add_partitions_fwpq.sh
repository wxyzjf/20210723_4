export DWHOME=${DWHOME:-/opt/reload}
export SCRIPTDIR=${DWHOME}/script
export TMPDIR=${DWHOME}/tmp

scriptname=`basename $0 |  cut -f1 -d'.'`
#. ${SCRIPTDIR}/dw_master_hds_reload.sh $scriptname N
. ${SCRIPTDIR}/dw_master_hds_reload_el.sh $scriptname N


jobmsg="Start $scriptname Add partitions"

startup "${jobmsg}"

optdate=`date +"%Y%m%d"`
filedate=`date -d "$optdate-1 day" +%Y%m%d`
#clsdate=`date -d "$optdate-5 day" +%Y%m%d`

#sed -i 's/$clsdate/$filedate/g' ${SQLDIR}/dw_add_partitions_fwpq.sql
sed -e "s/parm_date/$filedate/g" ${SQLDIR}/dw_add_partitions_fwpq.sql > ${TMPDIR}/dw_add_partitions_fwpq_tmp.sql

#echo "sed -i 's/$clsdate/$filedate/g' ${SQLDIR}/dw_add_partitions_fwpq.sql" | tee -a ${LOGFILE}
echo "sed -e "s/parm_date/$filedate/g" ${SQLDIR}/dw_add_partitions_fwpq.sql > ${TMPDIR}/dw_add_partitions_fwpq_tmp.sql" | tee -a ${LOGFILE}

echo "alter table h_fw_pq add partitions (trx_date='$filedate'); " | tee -a ${LOGFILE}

#runhivesql ${SQLDIR}/dw_add_partitions_fwpq.sql | tee -a ${LOGFILE}
runhivesql ${TMPDIR}/dw_add_partitions_fwpq_tmp.sql | tee -a ${LOGFILE}

if [ $? -ne 0 ];then
        echo "Add partitions Error at `date`\n" | tee -a ${LOGFILE}
        echo "Failure for ${scriptname} : Add partitions Error at `date`\n" | tee -a ${ERRFILE}
        maillist="eloise_wu@smartone.com"
        mailx -s "Failure for Add partitions" ${maillist} <<EOF
            Please check the job ${scriptname}, thanks.
EOF
        exit 1
fi

cleanup ${jobmsg}
exit 0





