#! /bin/ksh
#
# $Header: dw_master.ksh,v 1.2 2005/10/14 16:18:01 dwbat Exp $
#
# $Locker: dwbat $
#
# $Log:	dw_master.ksh,v $
# Revision 1.2  2005/10/14  16:18:01  16:18:01  dwbat (Data Warehouse Batch Job User (Oracle))
# New getcyclic to get call records table
# 

#
# Error handling
#
function errhandle {
  MVmsg="$1"
	maillist="kevin_ou@smartone.com rick_huang@smartone.com int_sara_luo@smartone.com"
  	##maillist="GZ-ISSupport@smartone.com DWSupport@smartone.com kevin_ou@smartone.com 98645372@mas.smartone.com 98645438@mas.smartone.com 64614596@mas.smartone.com stone_sherk@smartone.com"

##  echo "\n(BigInsight) Failure for $SCRIPTNAME : $MVmsg Error at `date`\n" | \
###    tee -a $MAINLOG

  echo -e "\nBP(BigInsight) Failure for $SCRIPTNAME : $MVmsg Error at `date`\n" |tee -a  $ERRFILE

 mutt -s "BP(BigInsight) Job failure: $SCRIPTNAME Error at `date`" $maillist<<EOFMAIL
$SCRIPTNAME : $MVmsg Error at `date`
EOFMAIL

}
function mailinfo {
  MVmsg="$1"
  MVsubject="BP(BigInsight) Job Info: $SCRIPTNAME at `date`"
  if [ $# -gt 1 ];then
  	MVsubject="$2"
  fi

  maillist="kevin_ou@smartone.com"
mutt -s "$MVsubject" $maillist<<EOMAIL
	$SCRIPTNAME : $MVmsg at `date`
EOMAIL
}
#####runsql in oracle ####
function runsql {
  sqlfile="$1"
  params="$2"

  sqlname=`basename $sqlfile`
  tmpsql=$TMPDIR/SQL/$sqlname

  echo "  `date` : Run SQL $sqlname with $params ...\c" | tee -a $LOGFILE
  echo "WHENEVER SQLERROR EXIT -1 ROLLBACK;" > $tmpsql
  if [ $? -ne 0 ]; then
    errhandle "SQL Init 1"
    exit 1
  fi
  cat $sqlfile >> $tmpsql
  if [ $? -ne 0 ]; then
    errhandle "SQL Init 2"
    exit 1
  fi
  echo "quit;" >> $tmpsql
  echo "/" >> $tmpsql
  if [ $? -ne 0 ]; then
    errhandle "SQL Init 3"
    exit 1
  fi

  sqlplus $DBUSER/$DBPASS@$DBSRV @$tmpsql $params 1>> $LOGFILE 2>> $ERRFILE
  if [ $? -ne 0 ]; then
    errhandle "Running SQL $sqlname"
    exit 1
  fi
  rm -f $tmpsql
  echo "Done" | tee -a $LOGFILE
}

function runbigsql {
  sqlfile="$1"
  params="$2"
  sqlname=`basename $sqlfile`
  tmpsql=$TMPDIR/SQL/$sqlname

  echo "\connect bigsql" > $tmpsql
  if [ $? -ne 0 ]; then
    errhandle "SQL Init 1"
    exit 1
  fi

  cat $sqlfile >> $tmpsql
  if [ $? -ne 0 ]; then
    errhandle "SQL Init 2"
    exit 1
  fi
  
  jsqsh -e -A -i $tmpsql 1>>$LOGFILE 2>>$LOGFILE
  if [ $? -ne 0 ];then
     echo "Runn jsqsh for uploading data error"|tee -a $LOGFILE
     errhandle "Runn jsqsh for uploading data error"
     exit 1
  fi 
	
}

function runhivesql {
  sqlfile="$1"
  params="$2"
  sqlname=`basename $sqlfile`
  tmpsql=$TMPDIR/SQL/$sqlname

  cat $sqlfile > $tmpsql
  if [ $? -ne 0 ]; then
    errhandle "SQL Init 1"
    exit 1
  fi

  hive -f $tmpsql 1>>$LOGFILE 2>>$LOGFILE
  if [ $? -ne 0 ];then
     echo "Run jsqsh for uploading data error"|tee -a $LOGFILE
     errhandle "Run jsqsh for uploading data error"
     exit 1
  fi
}

function runbigsql_noexit {
  sqlfile="$1"
  jsqsh -e -A -i $sqlfile 1>>$LOGFILE 2>>$LOGFILE
  if [ $? -ne 0 ];then
     echo "Runn jsqsh for uploading data error"|tee -a $LOGFILE
     errhandle "Runn jsqsh for uploading data error"
     ###exit 1
  fi

}
#
# Cleanup All Process
# 
function cleanup {
   MVmsg="$1"
   MVrmfiles="$2"

  date +"%Y-%m-%d" > $SCRIPTSTATUS
  rm -f $ERRFILE $PROCESS $MVrmfiles
  echo "$MVmsg Was Finished On `date`" | tee -a $LOGFILE
}

function waittime {
  MVchktime=$1

  echo "  `date` : Waiting for $MVchktime ...\c" | tee -a $LOGFILE 
  while [[ "`date +'%H%M'`" < "$MVchktime" ]]
  do
    sleep 300
  done
  echo "Done" | tee -a $LOGFILE
}

function getdate {
  MVday="$1"
  MVvariable="$2"
  MVformat="$3"

  echo "  `date` : Set today - $MVday to $MVvariable ...\c" | tee -a $LOGFILE
  if [ ! "$MVformat" ]; then
    MVformat="%Y-%m-%d"
  fi
  MVdate=`perl -e 'use Date::Manip; $date = DateCalc("today", "- '$MVday' days", \$err); print &UnixDate($date, "'$MVformat'");'`
  if [ $? -ne 0 ]; then
    errhandle "Get Date Error"
    exit 1
  fi
    
  eval "$MVvariable=$MVdate"
  echo "Done" | tee -a $LOGFILE
}

function getmonth {
  MVmonth="$1"
  MVvariable="$2"
  MVformat="$3"

  echo "  `date` : Set today - $MVmonth month to $MVvariable ...\c" | \
    tee -a $LOGFILE
  if [ ! "$MVformat" ]; then
    MVformat="%Y-%m-%d"
  fi
  MVdate=`perl -e 'use Date::Manip; $date = DateCalc("today", "- '$MVmonth' months", \$err); print &UnixDate($date, "'$MVformat'");'`
  if [ $? -ne 0 ]; then
    errhandle "Get Date Error"
    exit 1
  fi

  eval "$MVvariable=$MVdate"
  echo "Done" | tee -a $LOGFILE
}
function getmonth_firstday {
  MVmonth="$1"
  MVmonth=`expr 0 - $MVmonth `
  MVvariable="$2"
  MVformat="$3"

  echo "  `date` : Set today - $MVmonth month to $MVvariable ...\c" | \
    tee -a $LOGFILE
  if [ ! "$MVformat" ]; then
    MVformat="%Y-%m-%d"
  fi
  MVdate=`perl -e 'use Date::Manip; $date = DateCalc("today", "'$MVmonth' months", \$err); my $tmpdate = &UnixDate("$date", "%Y%m01");$date = DateCalc("$tmpdate","- 0 days", \$err);print &UnixDate($date, "'$MVformat'")'`
  if [ $? -ne 0 ]; then
    errhandle "Get Date Error"
    exit 1
  fi
  eval "$MVvariable=$MVdate"
  echo "Done" | tee -a $LOGFILE
}

function getmonth_lastday {
  MVmonth="$1"
  MVmonth=`expr 1 - $MVmonth `
  MVvariable="$2"
  MVformat="$3"
  echo "  `date` : Set today - $MVmonth month to $MVvariable ...\c" | \
    tee -a $LOGFILE
  if [ ! "$MVformat" ]; then
    MVformat="%Y-%m-%d"
  fi
  MVdate=`perl -e 'use Date::Manip; $date = DateCalc("today", "'$MVmonth' months", \$err); my $tmpdate = &UnixDate("$date", "%Y%m01");$date = DateCalc("$tmpdate","- 1 days", \$err);print &UnixDate($date, "'$MVformat'")'`
  if [ $? -ne 0 ]; then
    errhandle "Get Date Error"
    exit 1
  fi
  eval "$MVvariable=$MVdate"
  echo "Done" | tee -a $LOGFILE
}
#
# Check Lock
#
function checklock {
  MVlockfiles="$1"
  MVmessage="$2"

  MVlock=1
  MVsleepcount=0
  while [ $MVlock -eq 1 ]
  do
    MVlock=0
    echo "  `date` : $MVmessage ...\c" | tee -a $LOGFILE
    for MVfile in $MVlockfiles
    do
      if [ -r $MVfile ]; then
        MVlock=1
      fi
    done
    if [ $MVlock -eq 1 ]; then
      sleep 300
      MVsleepcount=`expr $MVsleepcount + 1`
    fi
    if [ $MVsleepcount -gt 144 ];  then
      errhandle "waiting lock"
      exit 1
    fi
    echo "Done" | tee -a $LOGFILE
  done
}
#
# 
# 
function startup {
   message="$1"
   waittime="$2"

   sleepcount=0

   echo "$message Was Started On `date`" | tee -a $LOGFILE

   while [ -f $PROCESSDIR/all.hold ]
   do
     sleep 900
   done

   if [ ! "$waittime" ]; then
     if [ -f $PROCESS ]; then
       errhandle "$SCRIPTNAME is running"
       exit 1
     fi
     touch $PROCESS $SCRIPTSTATUS
   else
     while [ -f $PROCESS ]
     do
       sleep $waittime

       if [ $sleepcount -gt 144 ]; then
         errhandle "waiting lock"
         exit 1	
       fi

       sleepcount=`expr $sleepcount + 1`
     done
     touch $PROCESS $SCRIPTSTATUS
   fi
}
#
# Make Directory
#
function md {
  MVdir="$1"

  mkdir -p $MVdir
  if [ $? -ne 0 ]; then
    errhandle "unable to make directory"
    exit 1
  fi
}
#
# Move Files
#
function move {
  MVsrc="$1"
  MVdes="$2"

  eval mv $MVsrc $MVdes
  if [ $? -ne 0 ]; then
    errhandle "unable to move from $MVsrc to $MVdes"
    exit 1
  fi
}
#
# Change Directory
#
function chgdir {
  MVcddir="$1"

  cd $MVcddir
  if [ $? -ne 0 ]; then
    errhandle "unable to change to $MVcddir"
    exit 1
  fi
}
#
#
#
#
#
function check_status {
  MVstatusfile=$PROCESSDIR/status/$1
  MVmessage=$2

  MVtoday=`date +"%Y-%m-%d"`
  MVcount=0

  echo "  `date` : $MVmessage ...\c" | tee -a $LOGFILE
  MVcompdate=`cat $MVstatusfile`
  while [[ "$MVcompdate" < "$MVtoday" ]]
  do
    sleep 180
    MVcount=`expr $MVcount + 1`
    if [ $MVcount -gt 200 ]; then
      errhandle "Timer Expired for $MVstatusfile"
      exit 1
    fi
    MVcompdate=`cat $MVstatusfile`
  done
  echo "Done" | tee -a $LOGFILE
}
#
# Update Program Status
#
function update_status {
  MVstatusfile=$PROCESSDIR/status/$1
  MVmessage=$2

  echo "  `date` : $MVmessage ...\c" | tee -a $LOGFILE
  echo `date +'%Y-%m-%d'` > $MVstatusfile
  if [ $? -ne 0 ]; then
    errhandle "Update Status $MVstatusfile Error"
    exit 1
  fi
  echo "Done" | tee -a $LOGFILE
}
#
#Check Complete File
#
function check_comfile {
scriptname="$1"

comfile=`grep "program:$scriptname:" $PARAMDIR/dw.par | cut -d':' -f1`
comfile=$PROCESSDIR/$comfile.completed
if [ ! -w $comfile ]; then
  errhandle "Completed File Has Not Been Found ..."
  exit 1
fi

}
##
#
# Dispatch source file into specify proc_folder
#
#
function dispatch_file_new {
        MVprocnum=$1
        MVsrcdir=$2
        MVdestdir=$3
        MVpattern=$4
        MVdatelst=$5

        MVproc_cnt=0

        chgdir ${MVsrcdir}
        cat /dev/null > ${TMPDIR}/${MVdatelst}
        cat /dev/null > ${TMPDIR}/tmp_${MVdatelst}
        echo "ls ${MVsrcdir} | grep ${MVpattern} | cut -c 8-15"|tee -a $LOGFILE
        for fdate in `ls ${MVsrcdir} | grep ${MVpattern} | cut -c 8-15`
        do
                echo $fdate >> ${TMPDIR}/tmp_${MVdatelst}
        done

        cat ${TMPDIR}/tmp_${MVdatelst} | uniq > ${TMPDIR}/${MVdatelst}

        for fd in `seq ${MVprocnum}`
        do
                if [ ! -d ${MVdestdir}/proc_${fd} ];then
                        mkdir ${MVdestdir}/proc_${fd}
                        if [ $? -ne 0 ];then
                                echo "failed to create directory $destdir/proc_$fd"|tee -a $LOGFILE
                                exit 0
                        fi
                fi

                while read line
                do
                        filecnt=`find ${MVdestdir}/proc_${fd} -name "${MVpattern}*" | wc -l`
                        if [ $filecnt -eq 0 ]; then
                                for f in `ls ${MVsrcdir} | grep $line`
                                do
                                        mv $f ${MVdestdir}/proc_${fd}
                                        if [ $? -ne 0 ];then
                                                echo "Dispatch File error"|tee -a $LOGFILE
                                                exit 1
                                        fi
                                done
                                continue
                        else
                                break
                        fi
                done < ${TMPDIR}/${MVdatelst}
        done

        echo "Dispatch File Finish"|tee -a $LOGFILE
}
function dispatch_file {
	MVprocnum=$1
	MVsrcdir=$2	
	MVdestdir=$3	
	MVpattern=$4

	MVproc_cnt=0;
	for fd in `seq $MVprocnum`
	do
		if [ ! -d ${MVdestdir}/proc_${fd} ];then
			mkdir ${MVdestdir}/proc_${fd}
			if [ $? -ne 0 ];then
				echo "failed to create directory $destdir/proc_$fd"|tee -a $LOGFILE
				exit 0
			fi
		fi
	done
	##for f in `ls -S $MVsrcdir/$MVpattern`
	chgdir $MVsrcdir
	for f in `find $MVsrcdir -maxdepth 1 -name "$MVpattern"`
	do
  		MVdcnt=`expr $MVproc_cnt % $MVprocnum + 1`
  		MVproc_cnt=`expr $MVproc_cnt + 1`
    		mv $f ${MVdestdir}/proc_${MVdcnt}
		if [ $? -ne 0 ];then
			echo "Dispatch File error"|tee -a $LOGFILE
			exit 0
		fi
	done
	echo "Dispatch File Finish"|tee -a $LOGFILE
}
function dispatch_file_head {
        MVprocnum=$1
        MVsrcdir=$2
        MVdestdir=$3
        MVpattern=$4
        MVhead=$5

        MVproc_cnt=0;
        for fd in `seq $MVprocnum`
        do
                if [ ! -d ${MVdestdir}/proc_${fd} ];then
                        mkdir ${MVdestdir}/proc_${fd}
                        if [ $? -ne 0 ];then
                                echo "failed to create directory $destdir/proc_$fd"|tee -a $LOGFILE
                                exit 0
                        fi
                fi
        done
        ##for f in `ls -S $MVsrcdir/$MVpattern`
        chgdir $MVsrcdir
        for f in `find $MVsrcdir  -maxdepth 1 -name "$MVpattern"|sort|head -n${MVhead}`
        do
                MVdcnt=`expr $MVproc_cnt % $MVprocnum + 1`
                MVproc_cnt=`expr $MVproc_cnt + 1`
                mv $f ${MVdestdir}/proc_${MVdcnt}
                if [ $? -ne 0 ];then
                        echo "Dispatch File error"|tee -a $LOGFILE
                        exit 0
                fi
        done
        echo "Dispatch File Finish"|tee -a $LOGFILE
}
#
#
# wait job finish 
#
#
#
function wait_subproc {
	for pid in $(jobs -p)
	do
		wait $pid 
		if [ $? -ne 0 ];then
			echo "$scriptname Sub Process abort"|tee -a $LOGFILE
			errhandle "$scriptname Sub Process abort,Please cehck"
			exit 1
		fi	
	done
}

#
# Trim Trailing Space
#
function trimspace {
  filelist="$1"
  rightspace="$2"

  if [ ! "$rightspace" ]; then
    rightspace="A"
  fi

  for file in $filelist
  do
    echo "  `date` : Trimming $file ...\c" | tee -a $LOGFILE
    if [ $rightspace = "R" ]; then
      perl -i -pe 's/ +\|/|/g; s/ +$//;' $file
      trimstatus=$?
    else
      perl -i -pe 's/ +\|/|/g; s/\| +/|/g; s/ +$//;' $file
      trimstatus=$?
    fi
    if [ $trimstatus -ne 0 ]; then
      errhandle "trim $file"
      exit 1
    fi
    echo "Done" | tee -a $LOGFILE
  done
}
#
#

function unloaddata {
  MVsqlfile="$1"
  MVdatafile="$2"
  MVcolsep="$3"
  MVtrimspace="$4"
  MVparams="$5"

  if [ ! "$MVcolsep" ]; then
    MVcolsep='|'
  fi
  if [ ! "$MVtrimspace" ]; then
    MVtrimspace="Y"
  fi
  MVsqlname=`basename $MVsqlfile`
  MVtmpsql=$TMPDIR/$MVsqlname

  cat<<eospool 1> $MVtmpsql 2>> $ERRFILE
set verify off
set echo off
set pagesize 0
set feedback off
set heading off
set term off
set colsep $MVcolsep
set linesize 10000
set trimspool on
set trim on
spool $MVdatafile
eospool
  if [ $? -ne  0 ]; then
    errhandle "SPOOL 1"
    exit 1
  fi

  cat $MVsqlfile >> $MVtmpsql
  if [ $? -ne  0 ]; then
    errhandle "SPOOL 2"
    exit 1
  fi

  runsql $MVtmpsql "$MVparams"

  if [ $MVtrimspace = "Y" ]; then
    trimspace $MVdatafile
  fi

  rm -f $MVtmpsql
}



function loaddata {
  ctrlfile="$1"
  datafile="$2"
  checkdiscardonly="$3"

  loadname=`basename $ctrlfile .ctl`
  echo "  `date` : Loading $loadname  ...\c" | tee -a $LOGFILE
  if [ "$checkdiscardonly" = "D" ]; then
    rm -f $CTLDIR/$loadname.bad 1>> $ERRFILE 2>&1
  fi
  if [ "$datafile" ]; then
    datafile="data=$datafile"
  fi   
  echo "sqlldr control=$ctrlfile log=$CTLDIR/$loadname.log \
    $datafile bad=$CTLDIR/$loadname.bad discard=$CTLDIR/$loadname.dis \
    userid=$DBUSER/$DBPASS@$DBSRV 1>> $LOGFILE 2>> $ERRFILE"

  sqlldr control=$ctrlfile log=$CTLDIR/$loadname.log \
    $datafile bad=$CTLDIR/$loadname.bad discard=$CTLDIR/$loadname.dis \
    userid=$DBUSER/$DBPASS@$DBSRV 1>> $LOGFILE 2>> $ERRFILE
  loadstatus=$?
  if [ "$checkdiscardonly" = "D" ]; then
    if [ -f $CTLDIR/$loadname.bad ]; then
      errhandle "Loading $loadname"
      exit 1
    fi
  else
    if [ $loadstatus -ne 0 ]; then
      errhandle "Loading $loadname"
      exit 1
    fi
  fi
  echo "Done" | tee -a $LOGFILE
}
##
##
##
##
function runscript {
  MVsfile="$1"
  MVmessage="$2"
  MVruninbg="$3"
  MVstoperr="$4"
  MVparams="$5"
  MVoutfile="$6"

  if [ ! "$MVoutfile" ]; then
    MVoutfile=$ERRFILE
  fi

  echo "  `date` : $MVmessage (Running $MVsfile) ...\c" | tee -a $LOGFILE
  if [ ! -x $SCRIPTDIR/$MVsfile ]; then
    errhandle "$MVsfile is not executable"
    exit 1
  fi
  if [ $MVruninbg -eq 1 ]; then
    eval "$SCRIPTDIR/$MVsfile $MVparams 1> $MVoutfile 2>> $LOGFILE &"
    if [ $? -ne 0 ]; then
      errhandle "fork process"
      if [ $MVstoperr -eq 1 ]; then
        exit 1
      fi
    fi
  else
    eval "$SCRIPTDIR/$MVsfile $MVparams 1> $MVoutfile 2>> $ERRFILE"
    if [ $? -ne 0 ]; then
      errhandle "running $MVsfile "
      if [ $MVstoperr -eq 1 ]; then
        exit 1
      fi
    fi
  fi
  echo "Done" | tee -a $LOGFILE
}
function checkJobResult {
        temp=`grep created $1 |head  -n4|cut -d" " -f1`
        eval set -A array $temp
        #echo ${array[0]}
        #echo ${array[1]}
        #echo ${array[2]}
        #echo ${array[3]}
        total=`expr ${array[1]} + ${array[2]} + ${array[3]}`
        echo "File---"$1
        if [ ${array[0]} -eq ${total} ];then
                echo "Check Job result equal to file."|tee -a $LOGFILE
        else
                echo "Check Job result record count abnormal(doesn't equal to file)."|tee -a $LOGFILE
                errhandle "Check Job result record count abnormal(doesn't equal to file)."
                exit 99
        fi
}

function checkdbstatus {
        cnt=0
        flag=`head -n1 $PROCESSDIR/db.hold`
        while [ $flag = "Y" ]
        do
                echo "waitting $cnt"
                cnt=`expr $cnt + 1`
                sleep 10
                if [ $cnt  -gt 3600 ];then
                        errhandle "Time out for waiting database"
                        exit 1
                fi
        flag=`head -n1 $PROCESSDIR/db.hold`
        done
}
function checkStopFlag {
	if [ -f ${PROCESSDIR}/control/$scriptname.stop ];then
                flg=`head -n1 ${PROCESSDIR}/control/$scriptname.stop`
                if [ $flg = "Y" ];then
                        echo "Found manual stop flag ,Exit"|tee -a $LOGFILE
                        cleanup "${jobmsg}"
                        exit 0
                fi
        fi
}

function adddate {
  MVday="$1"
  MVvariable="$2"
  MVformat="$3"

  echo "  `date` : Set today - $MVday to $MVvariable ...\c" | tee -a $LOGFILE
  if [ ! "$MVformat" ]; then
    MVformat="%Y-%m-%d"
  fi
  MVdate=`perl -e 'use Date::Manip; $date = DateCalc("today", "+ '$MVday' days", \$err); print &UnixDate($date, "'$MVformat'");'`
  if [ $? -ne 0 ]; then
    errhandle "Get Date Error"
    exit 1
  fi

  eval "$MVvariable=$MVdate"
  echo "Done" | tee -a $LOGFILE
}
function addmonth {
  MVmonth="$1"
  MVvariable="$2"
  MVformat="$3"

  echo "  `date` : Set today - $MVmonth month to $MVvariable ...\c" | \
    tee -a $LOGFILE
  if [ ! "$MVformat" ]; then
    MVformat="%Y-%m-%d"
  fi
  MVdate=`perl -e 'use Date::Manip; $date = DateCalc("today", "+ '$MVmonth' months", \$err); print &UnixDate($date, "'$MVformat'");'`
  if [ $? -ne 0 ]; then
    errhandle "Get Date Error"
    exit 1
  fi

  eval "$MVvariable=$MVdate"
  echo "Done" | tee -a $LOGFILE
}
function CheckFileCnt {
        MVsrcpath=$1
        MVfilepattern=$2
        MVfilenum=$3
        res=`find ${MVsrcpath}/ -name "${MVfilepattern}*"|wc -l`
        eval "${MVfilenum}=${res}"
}

function CheckIsRunning {
        MVscriptfile=$1
        MVcheckhours=$2
        MVres=$3
        MVruntimeout=$4
        MVscriptname=`basename $MVscriptfile |  cut -f1 -d'.'`
        MVprocess=${PROCESSDIR}/running/${MVscriptname}.running
        if [ -f $MVprocess ];then
        ###---Checking if the running exists too long----
                MVt1=`stat -c %Y $MVprocess`
                MVt2=`date +%s`
                MVdiff=`expr $MVt2 - $MVt1`
                ##MVhrs=`expr 3600 \* ${MVcheckhours}`
                MVhrs=`expr 1 \* ${MVcheckhours}`
                #### only send mail for once when MVruntimeout =0 to 1####
                if [ $MVdiff -gt $MVhrs ] ;then
                       ## send alert mail but doesn't exit ##
                	eval "$MVruntimeout=1"
                fi
                eval "$MVres=1"

        else
                eval "$MVres=0"
                eval "$MVruntimeout=0"
        fi
}

function BackupFile {
        bkgFile="$1"
        bkgDir="/nfs"
        dd=`date +'%d'`
        ymd=`date +'%Y%m%d'`
        if [ $dd -le 8 ];then
                bkgDir="/nfs/BIGINS_RAWFILE_Week1/BIGSQL_BACKUP/${ymd}"
        fi
        if [ $dd -ge 9 ] && [ $dd -le 16 ];then
                bkgDir="/nfs/BIGINS_RAWFILE_Week2/BIGSQL_BACKUP/${ymd}"
        fi
        if [ $dd -ge 17 ] && [ $dd -le 24 ];then
                bkgDir="/nfs/BIGINS_RAWFILE_Week3/BIGSQL_BACKUP/${ymd}"
        fi
        if [ $dd -ge 25 ];then
                bkgDir="/nfs/BIGINS_RAWFILE_Week4/BIGSQL_BACKUP/${ymd}"
        fi
        test -d ${bkgDir} || mkdir -p ${bkgDir}
        echo "Backup File ${bkgfile}"|tee -a $LOGFILE

        bkgTarFile=`basename ${bkgFile}`
        bkgTarFile=${bkgTarFile}".gz"

        gzip -c $bkgFile > ${bkgDir}/${bkgTarFile}
        if [ $? -ne 0 ];then
                echo "Backup File $bkgFile Error"|tee -a $LOGFILE
                errhandle "Backup File $bkgFile Error"|tee -a $LOGFILE
                exit 1
        fi
}

function waitfile {
  waitfpath=$1
  waitfname=$2
  waitfmin=$3

  echo "Wait COMPLETE FILE DATE:$waitfname with $waitfmin minutes $3"|tee -a $LOGFILE

  if [ $# -lt 2 ];then
        echo "Missing paramter for wait file script.(waitfpath waitfname waitfmin waitdays dateformat )"
        exit 99
  fi

  ctrl_flag=0
  sleepcount=0

  while [ $ctrl_flag -eq 0 ]
  do
    # Check every 1 minute
    if [ $sleepcount -ne 0 ];then
        sleep 60
    fi

    if [ $sleepcount -eq $waitfmin ]; then
       echo "Error, JOB is still running!\n"
       errhandle "Error, JOB is still running,waitting file [$waitfpath/$waitfname] timeout"
       exit 99
    fi

    res=`find $waitfpath -name $waitfname | wc -l`
    echo file count is $res | tee -a $LOGFILE
    if [ $res -eq 0 ]; then
       sleepcount=`expr $sleepcount + 1`
       echo "sleepcount + 1"
    else
      ctrl_flag=1
      echo "Complete file appear [ $waitfpath/$waitfname ] " |tee -a  $LOGFILE
    fi
  done
}

function waitfile_sftp {
  ftpsvc=$1
  ftpacc=$2
  waitfpath=$3
  waitfname=$4
  waitfmin=$5

  echo "Wait COMPLETE FILE DATE:$waitfname with $waitfmin minutes $3"|tee -a $LOGFILE

  if [ $# -lt 2 ];then
        echo "Missing paramter for wait file script.(waitfpath waitfname waitfmin waitdays dateformat )"
        #errhandle "Missing paramter for wait file script.(waitfpath waitfname waitfmin waitdays dateformat)"
        exit 99
  fi

  BPTMPDIR=$BILLIMGHOME/tmp
  ctrl_flag=0
  sleepcount=0

  waitcompletefile=${waitfname}
  rm  $DATADIR/$waitcompletefile

  while [ $ctrl_flag -eq 0 ]
  do
    # Check every 1 minute
    if [ $sleepcount -ne 0 ];then
        sleep 60
    fi

    if [ $sleepcount -eq $waitfmin ]; then
       echo "Error, JOB is still running!\n"
       errhandle "Error, JOB is still running,waitting file [$waitfpath/$waitcompletefile] timeout"
       exit 99
    fi

    chgdir  $DATADIR
    lftp sftp://$ftpsvc -u $ftpacc <<FOftp
        set ftp:ssl-allow true
        #set ftp:ssl-force true
        set ftp:ssl-protect-data true
        set ftp:ssl-protect-list true
        set ftps:initial-prot
        set xfer:clobber on
        cd $waitfpath
        mget $waitcompletefile
        quit
FOftp
    ###cp $waitfpath/$waitcompletefile $DATADIR/$waitcompletefile
    if [ ! -f $DATADIR/$waitcompletefile ]; then
       sleepcount=`expr $sleepcount + 1`
       echo "sleepcount + 1"
    else
       ctrl_flag=1
       echo "Complete file appear [ $waitfpath/$waitcompletefile ] " |tee -a  $LOGFILE
    fi
  done
}

function getfile_sftp {
  ftpsvc=$1
  ftpacc=$2
  srcfpath=$3
  srcfname=$4
  dstfpath=$5
  echo "Start to get file from $ftpsvc:$srcfpath/$srcfname"|tee -a $LOGFILE
  if [ $# -lt 5 ];then
        echo "Missing paramter for sftp file script.(ftpsvc ftpacc srcfpath srcfname dstfpath)"
        exit 99
  fi
  chgdir $dstfpath
  lftp sftp://$ftpsvc  -u $ftpacc <<FOftp
        set ftp:ssl-allow true
        #set ftp:ssl-force true
        set ftp:ssl-protect-data true
        set ftp:ssl-protect-list true
        set ftps:initial-prot
        set xfer:clobber on
        cd $srcfpath
        mget $srcfname
        quit
FOftp
  if [ ! -f $dstfpath/$srcfname ]; then
        echo "Failed to sftp file from src($ftpsvc:$srcfpath/$srcfname)"|tee -a $LOGFILE
        exit 99
  else
        echo "sFtp $srcfname complete" |tee -a $LOGFILE
  fi
}
function hbase_importTSV {
  MVjobcloumn="$1"
  MVtablename="$2"
  MVsrcfile="$3"
  MVdelemeter="$4"

  if [ $# -eq 4 ];then
  echo "  `date` : ==>Start Run $MVtablename under HBASE ...<==\c" | tee -a $LOGFILE
echo "java -cp hbase classpath org.apache.hadoop.hbase.mapreduce.ImportTsv -Dcreate.table=no -Dimporttsv.separator="$MVdelemeter" -Dimporttsv.skip.bad.lines=false -Dimporttsv.columns=$MVjobcloumn $MVtablename hdfs://$MVsrcfile" |tee -a $LOGFILE
    java -cp `hbase classpath` org.apache.hadoop.hbase.mapreduce.ImportTsv -Dcreate.table=no -Dimporttsv.separator="$MVdelemeter" -Dimporttsv.skip.bad.lines=false -Dimporttsv.columns=$MVjobcloumn $MVtablename hdfs://$MVsrcfile 1>> $LOGFILE 2>&1
else
  echo "  `date` : ==>Start Run $MVtablename under HBASE ...<==\c" | tee -a $LOGFILE
  echo "java -cp hbase classpath org.apache.hadoop.hbase.mapreduce.ImportTsv -Dcreate.table=no -Dimporttsv.skip.bad.lines=false -Dimporttsv.columns=$MVjobcloumn $MVtablename hdfs://$MVsrcfile" |tee -a $LOGFILE
  java -cp `hbase classpath` org.apache.hadoop.hbase.mapreduce.ImportTsv -Dcreate.table=no \
        -Dimporttsv.skip.bad.lines=false -Dimporttsv.columns=$MVjobcloumn $MVtablename hdfs://$MVsrcfile 1>> $LOGFILE 2>&1
fi	

  MVstatus=$?
  if [ $MVstatus -ne 0 ]; then
    echo "Run Error $MVtablename under HBASE"|tee -a $LOGFILE
    errhandle "Run Error $MVtablename under HBASE"
    exit 1
  fi
  echo "  `date` : ==>End Run $MVtablename under $HBASE ...Done...<==\c" | tee -a $LOGFILE
}

SCRIPTNAME=$1
#
# Define Data Mart Home Directory
#
DWHOME=${DWHOME:-/opt/reload/}
#
# Define necessary directories and setup
#
#NFSDIR=${NFSDIR:-/bpzfs01/BPDATA}

##----Script relate ----##

SCRIPTDIR=${DWHOME}/script
BINDIR=${DWHOME}/bin
SQLDIR=${DWHOME}/sql
TMPDIR=${DWHOME}/tmp
LOGDIR=${DWHOME}/log
PARAMDIR=${DWHOME}/param
CTLDIR=${DWHOME}/control
PROCESSDIR=${DWHOME}/process
STATUSDIR=${PROCESSDIR}/status
INPUTDIR=${DWHOME}/input

##----Job Running Relate----##
##OUTPUTDIR=/bpzfsout01/BPOUTPUT/output/
LDRDIR=${DWHOME}/ldrdata
DATADIR=${DWHOME}/cvtdata
LDRLISTDIR=${DWHOME}/ldrlist
SORTDIR=${DWHOME}/sortdata
SORTTMP=${DWHOME}/sorttmp


MAINLOG=${LOGDIR}/ERROR.LOG
#
# Program status files
#
SCRIPTSTATUS=$PROCESSDIR/status/$SCRIPTNAME.status
#
#Lock Files 
#
#
#DB Login
#DBUSER=bpbat
#DBPASS=Ids4728bt
#DBSRV=nbp_dw
#

umask 022

PATH=${PATH}:/usr/local/bin:/usr/local/mm/bin:/usr/contrib/bin:.:

set -o pipefail

#.  /opt/ibm/biginsights/conf/biginsights-env.sh

### export ORACLE_SID=modw
### export ORACLE_BASE=/opt/oracle1
### export ORACLE_HOME=/opt/oracle1/product/10.2.0
### export PATH=$ORACLE_HOME/bin:/usr/ccs/bin:$PATH:/modwapp01/users/modwbat/bin
### export TNS_ADMIN=/opt/oracle1/product/10.2.0/network/admin/${ORACLE_SID}
### export LD_LIBRARY_PATH=$ORACLE_HOME/lib:/usr/dt/lib:/usr/openwin/lib
### export SHLIB_PATH=$ORACLE_HOME/lib:/usr/lib:/modwapp01/users/modwbat/lib
### export NLS_LANG=american_america.WE8ISO8859P1
### export NLS_DATE_FORMAT=RRRR-MM-DD  
umask 022

export DWHOME SCRIPTDIR SQLDIR TMPDIR DATADIR BINDIR SRCDIR OUTPUTDIR LOGDIR \
       PARAMDIR PATH SHLIB_PATH LD_LIBRARY_PATH 

currdate=`date +"%Y%m%d%H%M%S"`
LOGFILE=${LOGDIR}/${SCRIPTNAME}_${currdate}.log
LSTFILE=${LOGDIR}/${SCRIPTNAME}_${currdate}.lst
ERRFILE=${LOGDIR}/${SCRIPTNAME}_${currdate}.err
PROCESS=${PROCESSDIR}/running/$SCRIPTNAME.running
#touch $LOGFILE $ERRFILE 
