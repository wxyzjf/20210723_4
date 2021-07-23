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

DELETE FROM ${etlvar::ADWDB}.BM_CNC_COMM_H where comm_mth = &comm_mth;
DELETE FROM ${etlvar::ADWDB}.BM_CNC_COMM_ROAM_H where comm_mth = &comm_mth;
commit;


-----Cover latest 4 mths  for bypass the later cases.
--------------------------------------------------------------------------------------------------------
prompt 'Step B_BM_CNC_COMM_H : [A. E-01 SIM Only Offer -renew with ld ] ';
insert into ${etlvar::ADWDB}.BM_CNC_COMM_H
(
     trx_mth
    ,comm_mth
    ,case_id
    ,case_src
    ,case_type
    ,cust_num
    ,subr_num
    ,acct_num
    ,subr_sw_on_date
    ,subr_sw_off_date
    ,min_subr_sw_on_date
    ,rate_plan_cd
    ,orig_rate_plan_cd
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_start_date
    ,ld_expired_date
    ,ld_orig_exp_date
    ,ld_nature
    ,dealer_cd
    ,split_acct
    ,split_subr
    ,skip_flg
    ,except_flg
    ,ld_inv_date
    ,cust_id_type
    ,ld_contract_period
    ,acct_mgr
    ,team_head
    ,comm_type2
    ,comm_type3
    ,plan_tariff
    ,cust_name
    ,idbr_prefix
    ,lm_cust_num
    ,lm_subr_num
    ,np_plan_tariff_lm
    ,np_plan_rebate_lm
    ,np_hs_subsidy_lm
    ,np_company_rebate_lm
    ,np_plan_fup_lm
    ,np_hsf_lm
    ,np_buyout_cost_lm
    ,np_lm_amt
    ,map_plan_type
    ,comm_rate
    ,base_comm
    ,net_roam_rev_amt_1m
    ,net_roam_rev_amt_2m
    ,net_roam_rev_amt_3m
    ,net_roam_rev_amt_lm
    ,roam_comm
    ,ttl_comm
    ,json_rmk
)
select 
     t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id
    ,t1.case_src
    ,t1.case_type
    ,t1.cust_num
    ,t1.subr_num
    ,t1.acct_num
    ,t1.subr_sw_on_date
    ,t1.subr_sw_off_date
    ,t1.min_subr_sw_on_date
    ,t1.rate_plan_cd
    ,t1.orig_rate_plan_cd
    ,t1.ld_inv_num
    ,t1.ld_cd
    ,t1.ld_mkt_cd
    ,t1.ld_start_date
    ,t1.ld_expired_date
    ,t1.ld_orig_exp_date
    ,t1.ld_nature
    ,t1.dealer_cd
    ,t1.split_acct
    ,t1.split_subr
    ,t1.skip_flg
    ,t1.except_flg
    ,t1.ld_inv_date
    ,t1.cust_id_type
    ,t1.ld_contract_period
    ,t1.acct_mgr
    ,t1.team_head
    ,t1.comm_type2
    ,t1.comm_type3
    ,t1.plan_tariff
    ,t1.cust_name
    ,t1.idbr_prefix
    ,t1.lm_cust_num
    ,t1.lm_subr_num
---- not yet ready
---- t3 ----Net plan calculate
    ,nvl(t3.np_plan_tariff_lm,0)
    ,nvl(t3.np_plan_rebate_lm,0)
    ,nvl(t3.np_hs_subsidy_lm,0)
    ,nvl(t3.np_company_rebate_lm,0)
    ,nvl(t3.np_plan_fup_lm,0)
    ,nvl(t3.np_hsf_lm,0)
    ,nvl(t3.np_buyout_cost_lm,0)
    ,nvl(t3.np_lm_amt,0)
---- t4 ----comm rate mapping 
    ,nvl(t4.map_plan_type,' ')
    ,nvl(t4.comm_rate,0)
    ,nvl(t4.base_comm,0)
---- t5 ----net roam 
    ,nvl(t5.net_roam_rev_amt_1m,0)
    ,nvl(t5.net_roam_rev_amt_2m,0)
    ,nvl(t5.net_roam_rev_amt_3m,0)
    ,nvl(t5.net_roam_rev_amt_lm,0)
    ,nvl(t5.ttl_roam_comm_amt,0) as roam_comm
    ,nvl(t5.ttl_roam_comm_amt,0) + nvl(t4.base_comm,0) ttl_comm
    ,nvl(t1.json_rmk,' ')||nvl(t3.json_rmk,' ') ||nvl(t4.json_rmk,' ') json_rmk
from ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T t1 
left outer join ${etlvar::TMPDB}.B_BM_CNC_COMM_003_T t3
        on t1.case_id = t3.case_id
left outer join ${etlvar::TMPDB}.B_BM_CNC_COMM_004_T t4
        on t1.case_id = t4.case_id
left outer join ${etlvar::TMPDB}.B_BM_CNC_COMM_005_T t5
        on t1.case_id = t5.case_id ;
commit;

insert into ${etlvar::ADWDB}.BM_CNC_COMM_ROAM_H
(    trx_mth
    ,comm_mth
    ,orig_comm_mth
    ,case_id
    ,case_type
    ,ld_inv_num
    ,ld_start_date
    ,lm_cust_num
    ,lm_subr_num
    ,lm_ld_expired_date
    ,orig_ld_expired_date
    ,ld_cd
    ,comm_rate
    ,seq
    ,net_roam_rev_amt_1m
    ,net_roam_rev_amt_2m
    ,net_roam_rev_amt_3m
    ,net_roam_rev_amt_lm
    ,roam_comm
    ,roam_comm_status
    ,acct_mgr
    ,team_head
    ,json_rmk
)
select 
     t5.trx_mth
    ,t5.comm_mth
    ,t5.orig_comm_mth
    ,t5.case_id
    ,t5.case_type
    ,t5.ld_inv_num
    ,t5.ld_start_date
    ,t5.lm_cust_num
    ,t5.lm_subr_num
    ,t5.lm_ld_expired_date
    ,t5.orig_ld_expired_date
    ,t5.ld_cd
    ,t5.comm_rate
    ,t5.seq
    ,t5.net_roam_rev_amt_1m
    ,t5.net_roam_rev_amt_2m
    ,t5.net_roam_rev_amt_3m
    ,t5.net_roam_rev_amt_lm
    ,t5.ttl_roam_comm_amt as roam_comm
    ,t5.roam_comm_status
    ,t5.acct_mgr
    ,t5.team_head
    ,t5.json_rmk
from  ${etlvar::TMPDB}.B_BM_CNC_COMM_005_T t5;
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

