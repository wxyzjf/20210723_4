######################################################
#i
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_BLACK_LIST_CUST/bin/b_black_list_cust0010.pl,v 1.1 2005/12/14 01:03:55 MichaelNg Exp $
#   Purpose:
#
#
######################################################

#my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_BM_SMS_COMM/bin/master_dev.pl";
my $ETLVAR = $ENV{"AUTO_ETLVAR"};
require $ETLVAR;

my $MASTER_TABLE = ""; #Please input the final target ADW table name here

sub runSQLPLUS{
    my $rc = open(SQLPLUS, "| sqlplus /\@${etlvar::TDDSN}");
    unless ($rc){
        print "Cound not invoke SQLPLUS commAND\n";
        return -1;
    }

    print SQLPLUS<<ENDOFINPUT;
        ${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}

--Please type your SQL statement here

set define on;
define tx_date = trunc(to_date('${etlvar::TXDATE}','YYYY-MM-DD'),'mm');
define trx_mth = add_months(&tx_date,-1);
define trx_e_date = add_months(&tx_date,0)-1;

--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'mig_adw.Y_BM_SMS_COMM_01_T1');
--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'mig_adw.Y_BM_SMS_COMM_01_T');
--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${etlvar::TMPDB}.B_BM_SMS_COMM_01_T1');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${etlvar::TMPDB}.B_BM_SMS_COMM_01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${etlvar::ADWDB}.BM_SMS_COMM');

--insert into MIG_ADW.Y_BM_SMS_COMM_01_T1
--insert into MIG_ADW.Y_BM_SMS_COMM_01_T
INSERT /*+ APPEND */ INTO ${etlvar::TMPDB}.B_BM_SMS_COMM_01_T
with tbl as (
select HKID_BR_PREFIX,
case when months_between(trunc(sysdate, 'mm'),&trx_mth) = 1
     then LM_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 2
     then L2M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 3
     then L3M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 4
     then L4M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 5
     then L5M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 6
     then L6M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 7
     then L7M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 8
     then L8M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 9
     then L9M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 10
     then L10M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 11
     then L11M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 12
     then L12M_NORMINATE_FLG
     when months_between(trunc(sysdate, 'mm'),&trx_mth) = 13
     then L13M_NORMINATE_FLG
     else CM_NORMINATE_FLG
end as NORMINATE_FLG
from PRD_BIZ_SUMM_VW.VW_KPI_ALL_CA_BR_LIST
)
select &trx_mth as TRX_MTH,
       tmpa.cust_num,
       nvl(tmpb.cust_name,' '),
       tmpa.subr_num,
       nvl(tmpb.HKID_BR_PREFIX,' ') as idbr_prefix,
       case when (tmpf.DEALER_CD = ' ') or (tmpf.DEALER_CD is null)
            then tmpd.DEALER_CD
       else tmpf.DEALER_CD
       end as DEALER_CD,
       nvl(tmpe.account_mgr,' ') as COMM_SALESMAN,
       nvl(tmpe.team_head,' ') as COMM_team,
       tmpa.subr_sw_on_date,
       tmpa.subr_sw_off_date,
       tmpa.RATE_PLAN_CD,
       SYSDATE,
       SYSDATE
from prd_adw.subr_info_hist tmpa
left outer join prd_adw.cust_info_hist tmpb
    on tmpa.cust_num = tmpb.cust_num
       and &trx_e_date between tmpb.start_date and tmpb.end_date
left outer join tbl tmpc
    on tmpb.HKID_BR_PREFIX = tmpc.HKID_BR_PREFIX
left outer join prd_biz_summ_vw.VW_PREPD_POSTPAID_SUBR_N1 tmpf
    on tmpa.cust_num = tmpf.cust_num
       and tmpa.subr_num = tmpf.subr_num
left outer join prd_biz_summ_vw.VW_ALL_PROFILING_N2 tmpd
    on tmpa.cust_num = tmpd.cust_num
       and tmpa.subr_num = tmpd.subr_num
left outer join prd_adw.subr_ac_mgr_hist tmpe
    on tmpb.HKID_BR_PREFIX = tmpe.IDBR_PREFIX
       and &trx_e_date between tmpe.start_date and tmpe.end_date
where &trx_e_date between tmpa.start_date and tmpa.end_date
      and tmpa.subr_stat_cd in ('OK','SU','PE')
      and tmpc.NORMINATE_FLG = 'Y'
      and tmpa.CUST_NUM  NOT  in (SELECT CUST_NUM FROM ${etlvar::ADWDB}.CUST_INFO_HIST WHERE CUST_TYPE_CD IN ('MBIRD','TBIRD','ZBIRD'));

COMMIT;

--INSERT /*+ APPEND */ INTO ${etlvar::TMPDB}.B_BM_SMS_COMM_01_T
--insert into MIG_ADW.Y_BM_SMS_COMM_01_T
--select tmpa.TRX_MTH,
--       tmpa.cust_num,
--       tmpa.cust_name,
--       tmpa.subr_num,
--       tmpa.idbr_prefix,
--       tmpa.DEALER_CD,
--       tmpa.COMM_SALESMAN,
--       tmpa.COMM_team,
--       tmpa.subr_sw_on_date,
--       tmpa.subr_sw_off_date,
--       tmpa.RATE_PLAN_CD,
--       SYSDATE,
--       SYSDATE
--from MIG_ADW.Y_BM_SMS_COMM_01_T1 tmpa
--where tmpa.DEALER_CD like 'CA%';

--commit;


insert into ${etlvar::ADWDB}.BM_SMS_COMM
(
  TRX_MTH,
  CUST_NUM,
  CUST_NAME,
  SUBR_NUM,
  IDBR_PREFIX,
  DEALER_CD,
  COMM_SALESMAN,
  COMM_TEAM,
  SUBR_SW_ON_DATE,
  SUBR_SW_OFF_DATE,
  RATE_PLAN_CD,
  CREATE_TS,
  REFRESH_TS
)select 
  TRX_MTH,
  CUST_NUM,
  CUST_NAME,
  SUBR_NUM,
  IDBR_PREFIX,
  DEALER_CD,
  COMM_SALESMAN,
  COMM_TEAM,
  SUBR_SW_ON_DATE,
  SUBR_SW_OFF_DATE,
  RATE_PLAN_CD,
  sysdate,
  sysdate
from ${etlvar::TMPDB}.B_BM_SMS_COMM_01_T;
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
my $ret = runSQLPLUS();
my $post = etlvar::postProcess();

exit($ret);






