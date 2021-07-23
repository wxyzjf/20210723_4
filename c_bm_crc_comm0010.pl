######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/APS
#   Purpose:
#   
#
######################################################


my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;

use DBI;
use Date::Manip;

#We need to have variable input for the program to start
if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("EXample: perl d_cust_info001.pl adw_d_cust_info_20051010.dir\n");
    exit(1);
}

my $MASTER_TABLE = ""; #Please input the final target ADW table name here
my $LOADING_TABLE_DB = "$etlvar::TMPDB"; #Please use the variable name defined in the etlvar

my $FTP_HOST;
my $FTP_PORT;
my $FTP_USERNAME;
my $FTP_PATH;

my $dbh = DBI->connect("dbi:Oracle:udsdwdb_adwetlbat", "", "", {RaiseError=>1, AutoCommit=> 0}) or die "$DBI::errstr\n";

$dbh->{ChopBlanks} = 1;
$dbh->{LongReadLen} = 4000;

my $sqlstmt = "
  select PULL_IP_ADDR,PULL_LOGIN,PULL_FTP_PORT,DATA_PATH
  from ${etlvar::ETLDB}.ETL_SRC_FILE
  where job_name = ?
";

my $dbcur = $dbh->prepare($sqlstmt);
$dbcur->bind_param(1,D_BM_CRC_COMM);
$dbcur->execute;
$dbcur->bind_columns(\$id_add, \$id_log, \$id_port, \$id_path);

while ($dbcur->fetch) {
  $FTP_HOST = $id_add;
  $FTP_USERNAME = $id_log;
  $FTP_PORT = $id_port;
  $FTP_PATH = $id_path;
}
$dbcur->finish;
$dbh->commit;
$dbh->disconnect;

#my $FTP_HOST="ftpsvc01";
#my $FTP_PORT="2026";
#my $FTP_USERNAME="smc/dw_ftp";
#my $FTP_PATH="/world/Teamwork/Tracking_Rpt/Standard_Pricing/SIMONLY";

my $FTP_PASSWD="dw000000";
my $filename;

sub lftp_get_filelist{
        system("lftp $FTP_HOST -u $FTP_USERNAME,$FTP_PASSWD -p $FTP_PORT <<FOftp
        set ftp:ssl-allow true
        set ftp:ssl-force true
        set ftp:ssl-protect-data true
        set ftp:ssl-protect-list true
        set ftps:initial-prot
        set xfer:clobber on
        set cmd:fail-exit
        cd $FTP_PATH
        ls $filename
        quit
FOftp
");
        $res=`echo $?`;
        if ($res != 0){
        EmailUser();
        return 3;
        }
}

sub EmailUser{

    print("\n\n\n#####################################\n");
    print("#  EMAIL FILE TO USER\n");
    print("#####################################\n");

        $SUBJECT="c_bm_crc_comm";
        $TOLIST="Eloise_Wu".q(@)."smartone.com";
        #$TOLIST="int_sara_luo".q(@)."smartone.com";
#       $TOLIST="edith_yeung".q(@)."smartone.com";
        my $rc = open(EMAIL_EOF, "| /usr/local/bin/mutt -s '${SUBJECT}' ${TOLIST}");
                unless ($rc){
                print "Cound not invoke mail command\n";
                return -1;
                }
    print EMAIL_EOF<<ENDOFINPUT;

please check the file $filename whether exist.

ENDOFINPUT
        close(EMAIL_EOF);

    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }else{
        return 0;
    }
}

sub initParam{
        $pre_date = &UnixDate("${etlvar::TXDATE}", "%Y-%m-%d");
        $touch_date = &UnixDate("${etlvar::TXDATE}", "%Y%m%d");
#       $touch_date = &UnixDate(DateCalc("${etlvar::TXDATE}", "+ 1 months", \$err), "%Y%m%d");
        $update_job_status_date = &UnixDate(DateCalc("${etlvar::TXDATE}", "- 1 months", \$err), "%Y-%m-%d");
        $file_date = &UnixDate("${etlvar::TXDATE}", "%y%m");
        #$filename="COMM_CAP_AMORT_$file_date*.xls";
        $filename="";
}

sub runSQLPLUS{
    my $rc = open(SQLPLUS, "| sqlplus /\@${etlvar::TDDSN}");
    unless ($rc){
        print "Cound not invoke SQLPLUS command\n";
        return -1;
    }
print SQLPLUS <<ENDOFINPUT;
        ${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}

--Please type your SQL statement here

--delete from ${etlvar::ETLDB}.ETL_RECEIVED_FILE where ETL_JOB='D_BM_CNC_COMM_PLAN_REF';

--update ${etlvar::ETLDB}.ETL_JOB set LAST_TXDATE=date'$update_job_status_date',LAST_JOBSTATUS='Ready' where ETL_JOB='D_BM_CNC_COMM_PLAN_REF';

--update ${etlvar::ETLDB}.ETL_JOB set LAST_TXDATE=date'$update_job_status_date',LAST_JOBSTATUS='Ready' where ETL_JOB='B_RPT_SIMO_REV';
--update ${etlvar::ETLDB}.ETL_JOB set LAST_TXDATE=date'$update_job_status_date',LAST_JOBSTATUS='Ready' where ETL_JOB='U_RPT_SIMO_REV';

--update ${etlvar::ETLDB}.ETL_SRC_FILE set LAST_PROCESS_START_TS=null where job_name='D_BM_CNC_COMM_PLAN_REF';

commit;

ENDOFINPUT
        close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }else{
        return 0;
    }
}

sub getCurrentTime{

        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time());
        $year += 1900;
        $mon = sprintf("%02d", $mon+1);
        $mday = sprintf("%02d", $mday);
        $hour = sprintf("%02d",$hour);
        $min = sprintf("%02d",$min);
        $sec = sprintf("%02d",$sec);
        $LOADTIME = "${year}-${mon}-${mday}";

        return $LOADTIME;
}

sub monthDifference{

        my ($curMonth, $prevMonth) = @_;
        my $curMonthNum = &UnixDate("$curMonth", "%m");
        my $curYearNum = &UnixDate("$curMonth", "%Y");
        my $prevMonthNum = &UnixDate("$prevMonth", "%m");
        my $prevYearNum = &UnixDate("$prevMonth", "%Y");

        my $curTotalMonth = ($curYearNum * 12) + $curMonthNum;
        my $prevTotalMonth = ($prevYearNum * 12) + $prevMonthNum;

        my $difference = $curTotalMonth - $prevTotalMonth;

        if ($difference < 0){
                print("Month difference < 0\n");
                exit(1);
        }else{
                return $difference;
        }
}




#Call the function we want to run
open(STDERR, ">&STDOUT");

my $pre = etlvar::preProcess($ARGV[0]);

etlvar::genFirstDayOfMonth($etlvar::TXDATE);
initParam();


my $curMonth = getCurrentTime();
my $diff = monthDifference(${curMonth},${pre_date});

#my $ret =lftp_get_filelist();
print "-get file error--------------------------------------------";

if ($ret == 0){$ret=runSQLPLUS();}
if ($ret == 0){
        #system("touch /opt/etl/prd/etl/preprocess/USR/rerun/d_bm_cnc_comm_plan_ref.rerun.'$touch_date' ");
#print "$update_job_status_date\n";
#print("substr($etlvar::TXDATE,8)\n");
#print "${etlvar::TXDATE};
}
my $post = etlvar::postProcess();
exit($ret);



