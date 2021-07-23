######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29 
#
#
######################################################


my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
##my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_BM_CNC_COMM/bin/master_dev.pl";
##require $ETLVAR;

my $MASTER_TABLE = ""; #Please input the final target ADW table name here

sub runSQLPLUS{
    my $rc = open(SQLPLUS, "| sqlplus /\@${etlvar::TDDSN}");
    ##my $rc = open(SQLPLUS, "| cat > a.sql");
    unless ($rc){
        print "Cound not invoke SQLPLUS command\n";
        return -1;
    }


    print SQLPLUS<<ENDOFINPUT;
        --${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}
set echo on
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
set linesize 2000
alter session force parallel query parallel 30;
alter session force parallel dml parallel 30;

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_002_T');

-----sample for date---
---- ld : 2020-09
---- comm paid at 2021-01
---- last mth of comm paid snapshot at 2020-12-31

set define on;
define tx_date = trunc(to_date('$etlvar::TXDATE','YYYY-MM-DD'),'MM');
define comm_mth = add_months(&tx_date,-1);
define trx_mth = add_months(&comm_mth,-4);
define trx_s_date = add_months(&comm_mth,-4);
define trx_e_date = add_months(&comm_mth,-3)-1;

-----Cover latest 4 mths  for bypass the later cases.
--------------------------------------------------------------------------------------------------------
prompt 'Step B_BM_CNC_COMM_002A01_T : [A. E-01 SIM Only Offer -renew with ld ] ';

insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_002_T
(
     FIN_PLAN_TYPE
    ,RATE_PLAN_CD
    ,PLAN_CAT
    ,LOCAL_DATA_USG
    ,SUPPORTLTE
    ,FUP_AC_UNLIMIT
    ,map_plan_type
    ,create_ts
    ,refresh_ts
)
select distinct 
     FIN_PLAN_TYPE
    ,RATE_PLAN_CD
    ,PLAN_CAT
    ,LOCAL_DATA_USG
    ,SUPPORTLTE
    ,FUP_AC_UNLIMIT
    ,case when r.fin_plan_type ='BT'
           and lower(r.plan_cat) in lower('4g/3g')
           and lower( r.supportlte) not like lower('%LTE % 5G')
           and lower(r.local_data_usg) like '%pool%'
           and lower(r.local_data_usg) not like '%hk%'
           and lower(r.local_data_usg) not like '%macau%'
           and lower(r.local_data_usg) not like '%unlimited%'
          then '4G_GB_POOL'
          when r.fin_plan_type ='BT'
           and lower(r.plan_cat) in lower('4G/3G')
           and lower( r.supportlte) not like lower('%LTE % 5G')
           and lower(r.local_data_usg) not like '%pool%'
           and lower(r.local_data_usg) not like '%hk%'
           and lower(r.local_data_usg) not like '%macau%'
           and lower(r.local_data_usg) not like '%unlimited%'
          then '4G_GB_FUP_AC'
          when r.fin_plan_type ='BT'
           and lower( r.supportlte) like lower('%LTE % 5G')
           and lower(r.local_data_usg) not like '%pool%'
           and lower(r.local_data_usg) not like '%hk%'
           and lower(r.local_data_usg) not like '%macau%'
           and lower(r.local_data_usg) not like '%unlimited%'
           and lower(r.fup_ac_unlimit) not like '%pool%'
           and lower(r.fup_ac_unlimit) not like '%hk%'
           and lower(r.fup_ac_unlimit) not like '%macau%'
           and lower(r.fup_ac_unlimit) not like '%unlimited%'
          then '5G_GB_AC'
          when r.fin_plan_type in ('MBB','M2M')
          and lower( r.supportlte) not like lower('%LTE % 5G')
           and lower(r.plan_cat) in ('mbb','m2m')
           and lower(r.local_data_usg) like '%pool%'
           and lower(r.local_data_usg) not like '%hk%'
           and lower(r.local_data_usg) not like '%macau%'
           and lower(r.local_data_usg) not like '%unlimited%'
          then '4G_GB_POOL'
          when r.fin_plan_type in ('MBB','M2M')
          and lower( r.supportlte) not like lower('%LTE % 5G')
           and lower(r.plan_cat) in ('mbb','m2m')
           and lower(r.local_data_usg) not like '%pool%'
           and lower(r.local_data_usg) not like '%hk%'
           and lower(r.local_data_usg) not like '%macau%'
           and lower(r.local_data_usg) not like '%unlimited%'
          then '4G_GB_FUP_AC'
         else 'OTHERS'
     end
     MAP_PLAN_TYPE
    ,sysdate as CREATE_TS
    ,sysdate as REFRESH_TS
from ${etlvar::ADWDB}.BM_CNC_COMM_PLAN_REF r;
commit;


quit;
---------------------------------------------------------
commit;
  exit;
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
etlvar::genFirstDayOfMonth($etlvar::TXDATE);
my $ret = runSQLPLUS();
my $post = etlvar::postProcess();

exit($ret);

