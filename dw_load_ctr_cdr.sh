export DWHOME=${DWHOME:-/opt/reload}
export SCRIPTDIR=${DWHOME}/script

scriptname=`basename $0 |  cut -f1 -d'.'`
. ${SCRIPTDIR}/dw_master_hds_reload.sh $scriptname N

jobmsg="Start $scriptname Loading"

#startup "${jobmsg}"



#getdate 1 clsdate "%Y%m%d"
#getdate 0 optdate "%Y%m%d"
opttime=`date +"%Y%m%d%H%M%S"`

CTRDIR=${DATADIR}/CTR_CDR
CTRINPUTDIR=${INPUTDIR}/CTR_CDR
CTRDPHDIR=${INPUTDIR}/DISPATCH_DIR/CTR_CDR

echo "Check File Cnt for loading"

CheckFileCnt "${CTRINPUTDIR}" "${p_filepattern}" "filecnt"

#if [ $filecnt -lt 50000 ];then
#	echo "Check File Count $filecnt less 50000 skip"|tee -a $LOGFILE
#	cleanup ${jobmsg}	
#	exit 0
#fi
echo "Check File Count $filecnt"|tee -a $LOGFILE


PROC_NUM=3
for p in `seq $PROC_NUM`;
do
if [ $p -le $PROC_NUM ];then
  if [ ! -d "${CTRDIR}/process/proc_${p}" ];then
    mkdir -p ${CTRDIR}/process/proc_${p}
  fi
fi
done

#test -d ${CTRDPHDIR}/complete/$optdate || mkdir -p ${CTRDPHDIR}/complete/$optdate

#### Housekepp File ####
#rm -rf ${CTRDIR}/complete/*.csv
rm -rf ${CTRDIR}/CTR*gz

echo "[Step 1 Start Copy file .... ]  `date`" | tee -a $LOGFILE

dispatch_file_head ${PROC_NUM} $CTRINPUTDIR $CTRDIR/process "*.csv" 100000


echo "[Step 2 Start Converting .... ]  `date`" | tee -a $LOGFILE

for d in `seq $PROC_NUM`
do
{
  chgdir ${CTRDIR}/process/proc_$d
  proc_tag="p"$d"_"$opttime
  for fcvt in `find . -name "*.csv"`
  do
    awkfcvt=`basename $fcvt`
    awk -v proc_tag="$proc_tag" -v outpath="$CTRDIR" '
      BEGIN{
        f_name="";
      }
      {if(f_name != FILENAME) {
        f_name=FILENAME;
        split(f_name,a,/_/);
        f_date=substr(a[1],2,8);
	split(a[3],b,/:/)
        event_id=b[1];
        next;
      }
      print $0 | "gzip >> " outpath"/CTR_"f_date"_"event_id"_"proc_tag".csv.gz";
    }' $awkfcvt
    if [ $? -ne 0 ];then
      mv ${CTRDIR}/process/proc_${d}/$awkfcvt ${CTRDIR}/error
      echo "AWK convert file failed " | tee -a $LOGFILE
      errhandle "AWK convert file failed"
      exit 1
    else
      #mv ${CTRDPHDIR}/process/proc_${d}/$awkfcvt ${CTRDPHDIR}/complete/$optdate
      mv ${CTRDIR}/process/proc_${d}/$awkfcvt ${CTRDIR}/complete
    fi
  done
}&
done
wait_subproc

cat /dev/null >${TMPDIR}/dw_load_ctr_cdr.sql
chgdir ${CTRDIR}
for cvt_file_name in `ls *.csv.gz`
do
	event_id=`echo $cvt_file_name | cut -d'_' -f3`
	part_time=`echo $cvt_file_name | cut -d'_' -f2`

	echo "alter table h_ctr_cdr add if not exists partition (trx_date='$part_time',event_id='$event_id'); " >>${TMPDIR}/dw_load_ctr_cdr.sql
	echo "load data LOCAL inpath '${CTRDIR}/${cvt_file_name}' into table h_ctr_cdr partition(trx_date='$part_time',event_id='$event_id');" >>${TMPDIR}/dw_load_ctr_cdr.sql
done

echo "Start run hive sql `date`"| tee -a $LOGFILE
### Hive table housekeep ###
#getdate 180 housekeep "%Y%m%d"
#echo "alter table h_ctr_cdr drop partition (trx_date='$housekeep');" >> ${TMPDIR}/dw_load_ctr_cdr.sql
runhivesql ${TMPDIR}/dw_load_ctr_cdr.sql

cleanup ${jobmsg}
exit 0
