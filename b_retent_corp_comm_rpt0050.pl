######################################################
#   $Header: /CVSROOT/smartone/Code/ETL/APP/ADW/B_HS_STK_ALLOCATE/bin/b_hs_stk_allocate0010.pl,v 1.1 2005/12/14 01:03:59 MichaelNg Exp $
#   Purpose:
#
#
######################################################

#my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_B_RETENT_CORP_COMM_RPT/bin/master_dev.pl";
my $ETLVAR = $ENV{"AUTO_ETLVAR"};
require $ETLVAR;


my $MASTER_TABLE = ""; #Please input the final target ADW table name here

my $ENV;

my $OUTPUT_FILE_PATH, $DEST_DIR,
   $OUTPUT_FILE_NAME;

my $FTP_FROM_HOST,$FTP_FROM_USERNAME,$FTP_FROM_PASSWORD;
my $FTP_TO_HOST,$FTP_TO_PORT,$FTP_TO_USERNAME,$FTP_TO_PASSWORD,$FTP_TO_DEST_PATH;

my $PROCESS_DATE;
my $PROCESS_DATE_LAST_YYYYMM;
my $CURR_MONTH_START;
my $CURR_MONTH_END;
my $LAST_MONTH_START;
my $LAST_MONTH_END;
my $NEXT_MONTH_START;

#########################################################################################################
#########################################################################################################
#########################################################################################################

sub initParam{

    $ENV = $ENV{"ETL_ENV"};

    use Date::Manip;

    #my $PROCESS_DATE = &UnixDate("${etlvar::TXDATE}", "%Y%m%d");

    

    #my $PROCESS_DATE_YYYYMM = &UnixDate("${etlvar::TXDATE}", "%Y%m");
    #my $PROCESS_DATE_YYYYMM = "202011";
    my $PROCESS_DATE_YYYYMMDD = &UnixDate("${etlvar::TXDATE}", "%Y%m%d");
    my $FILE_DATE = &UnixDate(DateCalc("${etlvar::TXDATE}", "- 1 months", \$err), "%Y%m");


    # ------------------------------------------------------------------#
    #  Please define the parameters for this job below.                 #
    # ------------------------------------------------------------------#

    $OUTPUT_FILE_PATH = ${etlvar::ETL_OUTPUT_DIR}."/".${etlvar::ETLSYS}."/".${etlvar::ETLJOBNAME};
    #$OUTPUT_FILE_PATH = "/opt/etl/prd/etl/APP/ADW/Y_B_RETENT_CORP_COMM_RPT/bin/el";
    
        if (! -d $OUTPUT_FILE_PATH) {
            system("mkdir ${OUTPUT_FILE_PATH}");
        }

#   system("rm -f ${OUTPUT_FILE_PATH}/*.txt");

    $OUTPUT_FILE_NAME_1 = "RETENT_CORP_COMM_H_unload_".$FILE_DATE.".csv";
    $OUTPUT_COMPLETE_1 = "RETENT_CORP_COMM_H_unload_".$FILE_DATE.".csv.complete";
    $OUTPUT_FILE_NAME_2 = "exceptional_unload_".$FILE_DATE.".csv";
    $OUTPUT_COMPLETE_2 = "exceptional_unload_".$FILE_DATE.".csv.complete";

    if ($ENV eq "DEV")
    {
        ##  DEVELOPMENT  ##

        # PUT action
        $FTP_TO_HOST = "";                                          # Please define
        $FTP_TO_PORT = "";                                          # Please define
        $FTP_TO_USERNAME = "";                                      # Please define
        $FTP_TO_PASSWORD = "";                                      # Please define
        $FTP_TO_DEST_PATH = "";                                     # Please define

        # GET action   (ONLY  FOR  DEVELOPMENT)
        $FTP_FROM_HOST = "${etlvar::DSSVR}";
        $FTP_FROM_USERNAME = "${etlvar::DSUSR}";
        $FTP_FROM_PASSWORD = "${etlvar::DSPWD}";
    }
    else
    {
        ##  PRODUCTION  ##

        # PUT action
    $FTP_TO_HOST = "ftpsvc01";
    $FTP_TO_PORT = "2026";
    $FTP_TO_USERNAME = "SMC/dw_ftp";
    $FTP_TO_DEST_PATH = "/WORLD/TEAMWORK/Tracking_Rpt/BM_RPT_SUMM/OUTPUT";
    $FTP_PASSWD="dw000000";
    }
}

#########################################################################################################
#########################################################################################################
#########################################################################################################

sub runSQLPLUS_EXPORT{

  my $SQLCMD_FILE="${etlvar::AUTO_GEN_TEMP_PATH}b_retent_corp_comm_rpt0010_sqlcmd.sql";
  ##my $SQLCMD_FILE="/opt/etl/prd/etl/APP/ADW/Y_B_RETENT_CORP_COMM_RPT/bin/el/y_b_retent_corp_rpt0050_sqlcmd.sql";
  open SQLCMD, ">" . $SQLCMD_FILE || die "Cannot open file" ;

  print SQLCMD<<ENDOFINPUT;

        ${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}
--Please type your SQL statement here
set define on;
--set linesize 2000
--alter session force parallel query parallel 30;
--alter session force parallel dml parallel 30;
---------------------------------------------------------------------------------------------------
define tx_date = trunc(to_date('$etlvar::TXDATE','YYYY-MM-DD'),'MM');
define comm_mth = add_months(&tx_date,-1);
define rpt_mth = add_months(&comm_mth,-3);
define rpt_s_date = add_months(&comm_mth,-3);
define rpt_e_date = add_months(&comm_mth,-2)-1;

---------------------------------------------------------------------------------------------------
${etlvar::SPOOLOPT}
--set termout off;
--.SET RETLIMIT 0 200;
--SET LINE 2000;
set head off;
set verify off;
set trimspool on;
set newpage 0;
set pagesize 0;
set lines 2500;
set termout off;
set serveroutput off;
set feedback off;
set echo off;
set trimout on;
--set colsep |;
set timing off;


SPOOL '${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1}';


select
         'RPT_MTH'
  ||','||'COMM_MTH'
  ||','||'CASE_ID'
  ||','||'CUST_NUM'
  ||','||'SUBR_NUM'
  ||','||'LD_INV_NUM'
  ||','||'LD_CD'
  ||','||'LD_MKT_CD'
  ||','||'LD_START_DATE'
  ||','||'LD_EXPIRED_DATE'
  ||','||'OLD_LD_INV_NUM'
  ||','||'OLD_LD_CD'
  ||','||'OLD_LD_MKT_CD'
  ||','||'OLD_LD_START_DATE'
  ||','||'OLD_LD_EXPIRED_DATE'
  ||','||'WAIVED_FLG'
  ||','||'BILL_CYCLE'
  ||','||'SUBR_SW_ON_DATE'
  ||','||'SUBR_STAT_CD'
  ||','||'BILL_INV_LIST'
  ||','||'REBATE_AMT'
  ||','||'HS_SUBSIDY_AMT'
  ||','||'OLD_BILL_INV_LIST'
  ||','||'OLD_REBATE_AMT'
  ||','||'OLD_HS_SUBSIDY_AMT'
  ||','||'RATE_PLAN_CD'
  ||','||'BILL_RATE'
  ||','||'OLD_RATE_PLAN_CD'
  ||','||'OLD_BILL_RATE'
  ||','||'BM_CAT'
  ||','||'CONTRACT_MTH'
  ||','||'OLD_CONTRACT_MTH'
  ||','||'CHG_PLAN_FLG'
  ||','||'REMAIN_CONTRACT_MTH'
  ||','||'TCV'
  ||','||'POS_INV_NUM'
  ||','||'POS_MKT_CD'
  ||','||'POS_INV_DATE'
  ||','||'POS_INV_TAG'
  ||','||'POS_LD_CD'
  ||','||'REBATE_FLG'
  ||','||'OLD_REBATE_FLG'
  ||','||'SALESMAN_CD'
  ||','||'JSON_RMK'
from dual
union all
select 
         RPT_MTH
  ||','||COMM_MTH
  ||','||CASE_ID
--  ||',="'||CUST_NUM||'"'
--  ||',="'||SUBR_NUM||'"'
  ||','||CUST_NUM
  ||','||SUBR_NUM
  ||','||LD_INV_NUM
  ||','||LD_CD
  ||','||LD_MKT_CD
  ||','||LD_START_DATE
  ||','||LD_EXPIRED_DATE
  ||','||OLD_LD_INV_NUM
  ||','||OLD_LD_CD
  ||','||OLD_LD_MKT_CD
  ||','||OLD_LD_START_DATE
  ||','||OLD_LD_EXPIRED_DATE
  ||','||WAIVED_FLG
  ||','||BILL_CYCLE
  ||','||SUBR_SW_ON_DATE
  ||','||SUBR_STAT_CD
  ||','||BILL_INV_LIST
  ||','||REBATE_AMT
  ||','||HS_SUBSIDY_AMT
  ||','||OLD_BILL_INV_LIST
  ||','||OLD_REBATE_AMT
  ||','||OLD_HS_SUBSIDY_AMT
  ||','||RATE_PLAN_CD
  ||','||BILL_RATE
  ||','||OLD_RATE_PLAN_CD
  ||','||OLD_BILL_RATE
  ||','||BM_CAT
  ||','||CONTRACT_MTH
  ||','||OLD_CONTRACT_MTH
  ||','||CHG_PLAN_FLG
  ||','||REMAIN_CONTRACT_MTH
  ||','||TCV
  ||','||POS_INV_NUM
  ||','||POS_MKT_CD
  ||','||POS_INV_DATE
  ||','||POS_INV_TAG
  ||','||POS_LD_CD
  ||','||REBATE_FLG
  ||','||OLD_REBATE_FLG
  ||','||SALESMAN_CD
  ||','||replace(JSON_RMK,',','|')
from ${etlvar::ADWDB}.RETENT_CORP_COMM_H
where rpt_mth = &rpt_mth;


spool off;
---------------------------------------------------------------------------------------------------
${etlvar::SPOOLOPT}
set termout off;
--.SET RETLIMIT 0 200;
SET LINE 2000;
set head off;
set verify off;
set trimspool on;
set newpage 0;
set pagesize 0;
set lines 2500;
set termout off;
set serveroutput off;
set feedback off;
set echo off;
set trimout off;
set colsep |;
set timing off;
SPOOL '${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_2}';

select
         'INV_NUM'
  ||','||'INV_DATE'
  ||','||'CUST_NUM'
  ||','||'SUBR_NUM'
  ||','||'INV_TYPE_CD'
  ||','||'POS_SHOP_CD'
  ||','||'SALESMAN_CD'
  ||','||'DESPATCH_NUM'
  ||','||'OS_FLG'
  ||','||'ALLOCATE_FLG'
  ||','||'RMK'
  ||','||'ID_NUM'
  ||','||'USR_ID'
  ||','||'TIME_ISSUED'
  ||','||'STAT'
  ||','||'ON_CREDIT'
  ||','||'POS_SAL_CAT_CD'
  ||','||'CREDIT_CD'
  ||','||'PER_DIVIDED'
  ||','||'START_MONTH'
  ||','||'BONUS'
  ||','||'GR_ACK_DATE'
  ||','||'LVP_FLG'
  ||','||'POS_APPR_CD'
  ||','||'NETW_IND'
  ||','||'MKT_CD'
  ||','||'POS_INSTALL_CD'
  ||','||'PLAN_A1_FLG'
  ||','||'NET_CHG_IND'
  ||','||'SA_NUM'
  ||','||'ACT_DOC'
  ||','||'NODOCUMENT_REASON'
  ||','||'ROADSHOW_FLG'
  ||','||'LD_CD'
  ||','||'INV_TOT_AMT'
  ||','||'INV_OS_AMT'
  ||','||'REFUND_AMT'
  ||','||'VOID_AMT'
  ||','||'CREDIT_TOT_AMT'
  ||','||'CREATE_TS'
  ||','||'UNIT_CRBK_AMT'
  ||','||'REFRESH_TS'
  ||','||'LD_EXPIRY_DATE'
  ||','||'RATE_PLAN_CD'
  ||','||'DEALER_BOOTH'
  ||','||'DOA_INVOICE'
  ||','||'STAFF_ID'
  ||','||'PROMOTER_ID'
  ||','||'PROMOTER_NAME'
  ||','||'BUYOUT_NO'
  ||','||'VOICE_MSISDN'
  ||','||'VOICE_SIM'
  ||','||'REFERRER_MSISDN'
  ||','||'CUST_CATEGORY'
  ||','||'REFEREE_SUBR_NUM'
  ||','||'REFEREE_CUST_NUM'
  ||','||'REFEREE_CUST_NAME'
  ||','||'REFEREE_TYPE'
  ||','||'LOG_NO'
  ||','||'SA_PLAN_CD'
  ||','||'SA_OLD_PLAN'
  ||','||'ONLINE_INV_FLG'
  ||','||'ECOMM_REF_ID'
  ||','||'ECOMM_ORDER_ID'
  ||','||'REBATE_BANK_FLG'
  ||','||'ONLINE_RETENT_PORTAL'
  ||','||'CASE_ID'
  ||','||'PO_NO'
  ||','||'CONTACT_PERSON'
  ||','||'CONTACT_NUM'
from dual
union all
select
         INV_NUM
  ||','||INV_DATE
  ||','||CUST_NUM
  ||','||SUBR_NUM
  ||','||INV_TYPE_CD
  ||','||POS_SHOP_CD
  ||','||SALESMAN_CD
  ||','||DESPATCH_NUM
  ||','||OS_FLG
  ||','||ALLOCATE_FLG
  ||','||RMK
  ||','||ID_NUM
  ||','||USR_ID
  ||','||TIME_ISSUED
  ||','||STAT
  ||','||ON_CREDIT
  ||','||POS_SAL_CAT_CD
  ||','||CREDIT_CD
  ||','||PER_DIVIDED
  ||','||START_MONTH
  ||','||BONUS
  ||','||GR_ACK_DATE
  ||','||LVP_FLG
  ||','||POS_APPR_CD
  ||','||NETW_IND
  ||','||MKT_CD
  ||','||POS_INSTALL_CD
  ||','||PLAN_A1_FLG
  ||','||NET_CHG_IND
  ||','||SA_NUM
  ||','||ACT_DOC
  ||','||NODOCUMENT_REASON
  ||','||ROADSHOW_FLG
  ||','||LD_CD
  ||','||INV_TOT_AMT
  ||','||INV_OS_AMT
  ||','||REFUND_AMT
  ||','||VOID_AMT
  ||','||CREDIT_TOT_AMT
  ||','||CREATE_TS
  ||','||UNIT_CRBK_AMT
  ||','||REFRESH_TS
  ||','||LD_EXPIRY_DATE
  ||','||RATE_PLAN_CD
  ||','||DEALER_BOOTH
  ||','||DOA_INVOICE
  ||','||STAFF_ID
  ||','||PROMOTER_ID
  ||','||PROMOTER_NAME
  ||','||BUYOUT_NO
  ||','||VOICE_MSISDN
  ||','||VOICE_SIM
  ||','||REFERRER_MSISDN
  ||','||CUST_CATEGORY
  ||','||REFEREE_SUBR_NUM
  ||','||REFEREE_CUST_NUM
  ||','||REFEREE_CUST_NAME
  ||','||REFEREE_TYPE
  ||','||LOG_NO
  ||','||SA_PLAN_CD
  ||','||SA_OLD_PLAN
  ||','||ONLINE_INV_FLG
  ||','||ECOMM_REF_ID
  ||','||ECOMM_ORDER_ID
  ||','||REBATE_BANK_FLG
  ||','||ONLINE_RETENT_PORTAL
  ||','||CASE_ID
  ||','||PO_NO
  ||','||CONTACT_PERSON
  ||','||CONTACT_NUM
from prd_adw.pos_inv_header i
where i.inv_date between &rpt_s_date and &rpt_e_date
and salesman_cd in(select SALESMAN_CD from ${etlvar::ADWDB}.retent_corp_comm_salesman
                   where COMM_MTH = &comm_mth)
and i.inv_num not in(
        select r.inv_num from prd_adw.pos_return_header r
        where r.trx_date between &rpt_s_date and &rpt_e_date
)and i.inv_num not in (
    select  ld_inv_num
    from ${etlvar::TMPDB}.b_retent_corp_comm_004B01_t rs_ta
);

spool off;

---------------------------------------------------------------------------------------------------

COMMIT;

exit;

ENDOFINPUT
  close(SQLCMD);
  print("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  my $ret = system("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  if ($ret != 0)
  {
    return (1);
  }

}


# EMAIL TO User
sub EmailUser{

    print("\n\n\n#####################################\n");
    print("#  Email user\n");
    print("#####################################\n");

    my $TOLIST, $CCLIST, $BCCLIST, $SUBJECT, $ATTACH;

    if ($ENV eq "DEV")
    {
        #$TOLIST = "woody_kwok".q(@)."smartone.com";
        $TOLIST = "Eloise_Wu".q(@)."smartone.com";
        #$CCLIST = "Kevin_ou".q(@)."smartone.com";
        #$BCCLIST = "";
        $ATTACH = "-a ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1} -a ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_2}";
    }
     else
    {
        #$TOLIST = etlvar::getEmailList("TOLIST", ${etlvar::ETLJOBNAME});
        #$CCLIST = etlvar::getEmailList("CCLIST", ${etlvar::ETLJOBNAME});
        #$BCCLIST = etlvar::getEmailList("BCCLIST", ${etlvar::ETLJOBNAME});

        ##$TOLIST = "joe_chan".q(@)."smartone.com";
        ##$CCLIST = " ";
        ##$BCCLIST = " ";

        $TOLIST = "Eloise_Wu".q(@)."smartone.com";
        $ATTACH = "-a ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1} -a ${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_2}";
    }

    $SUBJECT = "CORP COMM Report on ${etlvar::TXDATE}";

    #my $rc = open(EMAIL_EOF, "| /usr/local/bin/mutt -s '${SUBJECT}'  ${CCLIST} ${BCCLIST} ${TOLIST} ${ATTACH}");
    my $rc = open(EMAIL_EOF, "| /usr/local/bin/mutt -s '${SUBJECT}' ${TOLIST} ${ATTACH}");
    unless ($rc){
            print "Cound not invoke mutt command\n";
            return -1;
    }

    print EMAIL_EOF<<ENDOFINPUT;
Please check the report file ${OUTPUT_FILE_NAME_1} and ${OUTPUT_FILE_NAME_2} which is generated on ${etlvar::TXDATE}.

ENDOFINPUT
    close(EMAIL_EOF);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }
}

sub sendFile{
    system("lftp $FTP_TO_HOST -p $FTP_TO_PORT -u $FTP_TO_USERNAME,$FTP_PASSWD<<FOftp
        set ssl:verify-certificate no
        set ftp:ssl-allow true
        set ftp:ssl-force false
        set ftp:ssl-protect-data true
        set ftp:ssl-protect-list true
        set ftps:initial-prot
        set xfer:clobber on
        cd $FTP_TO_DEST_PATH
        lcd $OUTPUT_FILE_PATH
        put $OUTPUT_FILE_NAME_1
        put $OUTPUT_FILE_NAME_2
        quit
FOftp
        ");
        my $RET_CODE = $? >> 8;
        if ($RET_CODE != 0){
                return 1;
                print("ret=1");
        }else{
                print("ret=0");
                return 0;
        }
}


#########################################################################################################
#########################################################################################################
#########################################################################################################

#We need to have variable input for the program to start
if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("Example: perl b_cust_info0010.pl adw_b_cust_info_20051010.dir\n");
    exit(1);
}

#Call the function we want to run
open(STDERR, ">&STDOUT");

my $pre = etlvar::preProcess($ARGV[0]);
my $rc = etlvar::getTXDate($MASTER_TABLE);
etlvar::genFirstDayOfMonth($etlvar::TXDATE);

## Disable the Perl buffer, Print the log message immediately
$| = 1;

initParam();

my $ret = 0;

##################################################################################

# RUN SQL and EXPORT FILE
if ($ret == 0)
{
    $ret = runSQLPLUS_EXPORT();
}

##################################################################################

## REMOVE FILE HEADER
if ($ret == 0)
{
    system("touch ${OUTPUT_FILE_PATH}/${OUTPUT_COMPLETE_1}");
    system("touch ${OUTPUT_FILE_PATH}/${OUTPUT_COMPLETE_2}");
}

if ($ret == 0)
{
  if (${etlvar::ENABLE_EMAIL} eq "Y") {
    $ret = EmailUser();
    if ($ret == 0){
       $ret = sendFile();
    }
  }
}


my $post = etlvar::postProcess();

exit($ret);



















