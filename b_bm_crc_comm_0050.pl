######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29 
#
#
######################################################


my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
##my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_BM_CRC_COMM/bin/master_dev.pl";
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

--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_005_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_005A01_T');
---- comm paid at 2021-01
---- last mth of comm paid snapshot at 2020-12-31

set define on;
define tx_date = trunc(to_date('$etlvar::TXDATE','YYYY-MM-DD'),'MM');
define comm_mth = add_months(&tx_date,-1);
define trx_mth = add_months(&comm_mth,-4);
define trx_s_date = add_months(&comm_mth,-4);
define trx_e_date = add_months(&comm_mth,-3)-1;

-------------Reserve the logic for deduct the lm ld overlap exceptional cases.
-----------
DELETE FROM ${etlvar::ADWDB}.BM_CRC_COMM_H where comm_mth = &comm_mth;
DELETE FROM ${etlvar::ADWDB}.BM_CRC_COMM_ROAM_H where comm_mth = &comm_mth;

insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_005A01_T    
   (trx_mth
    ,comm_mth
    ,case_id
    ,case_src
    ,case_type
    ,ld_cust_num
    ,ld_subr_num
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_start_date
    ,ld_expired_date
    ,ld_orig_exp_date
    ,ld_nature
    ,ld_inv_date
    ,om_order_id
    ,saleman_cd
    ,sale_team
    ,dealer_cd
    ,normin_flg
    ,cust_label_team
    ,split_subr
    ,cust_name
    ,min_subr_sw_on_date
    ,subr_sw_on_date
    ,subr_sw_off_date
    ,rate_plan_cd
    ,rate_plan_tariff
    ,skip_flg
    ,except_flg
    ,lm_cust_num
    ,lm_subr_num
    ,idbr_prefix
    ,contract_mth
    ,lm_ob_roam_data_rev_amt
    ,lm_rebate_amt
    ,np_plan_tariff_lm
    ,np_plan_rebate_lm
    ,np_hs_subsidy_lm
    ,np_company_subsidy_lm
    ,np_hsfund_lm
    ,np_plan_tariff_yoylm
    ,np_plan_rebate_yoylm
    ,np_hs_subsidy_yoylm
    ,np_company_subsidy_yoylm
    ,np_hsfund_yoylm
    ,bt_rev_avg3mth
    ,bt_rev_avg3mth_yoy
    ,np_lm_amt
    ,np_yoylm_amt
    ,np_lm_rev_amt
    ,np_plan_fup_lm
    ,np_plan_fup_yoylm
    ,net_roam_rev_amt_1m
    ,net_roam_rev_amt_2m
    ,net_roam_rev_amt_3m
    ,ttl_roam_comm_amt
    ,ttl_comm_amt
    ,comm_rate_flg
    ,comm_rate
    ,comm_pay_method
    ,prv_case_id
    ,prv_np_lm_rev_amt
    ,prv_ld_orig_exp_date
    ,prv_contract_mth
    ,json_rmk
)
select 
     t.trx_mth
    ,t.comm_mth
    ,t.case_id
    ,t.case_src
    ,case when br.case_id is not null then br.case_type else nvl(bt.case_type,' ') end  case_type
    ,t.ld_cust_num
    ,t.ld_subr_num
    ,t.ld_inv_num
    ,t.ld_cd
    ,t.ld_mkt_cd
    ,t.ld_start_date
    ,t.ld_expired_date
    ,t.ld_orig_exp_date
    ,t.ld_nature
    ,t.ld_inv_date
    ,t.om_order_id
    ,t.saleman_cd
    ,t.sale_team
    ,t.dealer_cd
    ,t.normin_flg
    ,t.cust_label_team
    ,t.split_subr
    ,t.cust_name
    ,t.min_subr_sw_on_date
    ,t.subr_sw_on_date
    ,t.subr_sw_off_date
    ,t.rate_plan_cd
    ,t.rate_plan_tariff
    ,t.skip_flg
    ,t.except_flg
    ,t.lm_cust_num
    ,t.lm_subr_num
    ,t.idbr_prefix
    ,t.contract_mth
    ,nvl(br.lm_ob_roam_data_rev_amt,0)
    ,nvl(br.lm_rebate_amt,0)
    ,nvl(bt.np_plan_tariff_lm,0)
    ,nvl(bt.np_plan_rebate_lm,0)
    ,nvl(bt.np_hs_subsidy_lm,0)
    ,nvl(bt.np_company_subsidy_lm,0)
    ,nvl(bt.np_hsfund_lm,0)
    ,nvl(bt.np_plan_tariff_yoylm,0)
    ,nvl(bt.np_plan_rebate_yoylm,0)
    ,nvl(bt.np_hs_subsidy_yoylm,0)
    ,nvl(bt.np_company_subsidy_yoylm,0)
    ,nvl(bt.np_hsfund_yoylm,0)
    ,nvl(bt.bt_rev_avg3mth,0)
    ,nvl(bt.bt_rev_avg3mth_yoy,0)
    ,nvl(case when br.case_id is not null then br.np_lm_amt else bt.np_lm_amt end,0) np_lm_amt
    ,nvl(bt.np_yoylm_amt,0)
    ,nvl(case when br.case_id is not null then br.np_lm_rev_amt else bt.np_lm_rev_amt end,0) np_lm_rev_amt
    ,nvl(bt.np_plan_fup_lm,0)
    ,nvl(bt.np_plan_fup_yoylm,0)
    ,nvl(ro.net_roam_rev_amt_1m,0)
    ,nvl(ro.net_roam_rev_amt_2m,0)
    ,nvl(ro.net_roam_rev_amt_3m,0)
    ,nvl(ro.ttl_roam_comm_amt,0)
    ,nvl(case when br.case_id is not null then br.ttl_comm_amt else bt.ttl_comm_amt end,0) ttl_comm_amt
    ,nvl(bt.comm_rate_flg,' ')
    ,nvl(case when br.case_id is not null then br.comm_rate else bt.comm_rate end,0) comm_rate
    ,' ' as comm_pay_method
    ,nvl(bt.prv_case_id,' ')
    ,nvl(bt.prv_np_lm_rev_amt,0)
    ,nvl(bt.prv_ld_orig_exp_date,date '1900-01-01')
    ,nvl(bt.prv_contract_mth,0)
    ,t.json_rmk||nvl(br.json_rmk,'')||nvl(bt.json_rmk,'') as json_rmk
from ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T t 
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_002_T br
    on t.case_id = br.case_id
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_003_T bt
    on t.case_id = bt.case_id
----- only keep seq 0 for first batch paid    
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_004_T ro
    on t.case_id = ro.case_id;
commit;

----# override commission if found skip_flag is 0 
insert into ${etlvar::ADWDB}.BM_CRC_COMM_H
   (trx_mth
    ,comm_mth
    ,case_id
    ,case_src
    ,case_type
    ,ld_cust_num
    ,ld_subr_num
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_start_date
    ,ld_expired_date
    ,ld_orig_exp_date
    ,ld_nature
    ,ld_inv_date
    ,om_order_id
    ,saleman_cd
    ,sale_team
    ,dealer_cd
    ,normin_flg
    ,cust_label_team
    ,split_subr
    ,cust_name
    ,min_subr_sw_on_date
    ,subr_sw_on_date
    ,subr_sw_off_date
    ,rate_plan_cd
    ,rate_plan_tariff
    ,skip_flg
    ,except_flg
    ,lm_cust_num
    ,lm_subr_num
    ,idbr_prefix
    ,contract_mth
    ,lm_ob_roam_data_rev_amt
    ,lm_rebate_amt
    ,np_plan_tariff_lm
    ,np_plan_rebate_lm
    ,np_hs_subsidy_lm
    ,np_company_subsidy_lm
    ,np_hsfund_lm
    ,np_plan_tariff_yoylm
    ,np_plan_rebate_yoylm
    ,np_hs_subsidy_yoylm
    ,np_company_subsidy_yoylm
    ,np_hsfund_yoylm
    ,bt_rev_avg3mth
    ,bt_rev_avg3mth_yoy
    ,np_lm_amt
    ,np_yoylm_amt
    ,np_lm_rev_amt
    ,np_plan_fup_lm
    ,np_plan_fup_yoylm
    ,net_roam_rev_amt_1m
    ,net_roam_rev_amt_2m
    ,net_roam_rev_amt_3m
    ,ttl_roam_comm_amt
    ,ttl_comm_amt
    ,comm_rate_flg
    ,comm_rate
    ,comm_pay_method
    ,prv_case_id
    ,prv_np_lm_rev_amt
    ,prv_ld_orig_exp_date
    ,prv_contract_mth
    ,json_rmk
)select
trx_mth
    ,comm_mth
    ,case_id
    ,case_src
    ,case_type
    ,ld_cust_num
    ,ld_subr_num
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_start_date
    ,ld_expired_date
    ,ld_orig_exp_date
    ,ld_nature
    ,ld_inv_date
    ,om_order_id
    ,saleman_cd
    ,sale_team
    ,dealer_cd
    ,normin_flg
    ,cust_label_team
    ,split_subr
    ,cust_name
    ,min_subr_sw_on_date
    ,subr_sw_on_date
    ,subr_sw_off_date
    ,rate_plan_cd
    ,rate_plan_tariff
    ,skip_flg
    ,except_flg
    ,lm_cust_num
    ,lm_subr_num
    ,idbr_prefix
    ,contract_mth
    ,lm_ob_roam_data_rev_amt
    ,lm_rebate_amt
    ,np_plan_tariff_lm
    ,np_plan_rebate_lm
    ,np_hs_subsidy_lm
    ,np_company_subsidy_lm
    ,np_hsfund_lm
    ,np_plan_tariff_yoylm
    ,np_plan_rebate_yoylm
    ,np_hs_subsidy_yoylm
    ,np_company_subsidy_yoylm
    ,np_hsfund_yoylm
    ,bt_rev_avg3mth
    ,bt_rev_avg3mth_yoy
    ,np_lm_amt
    ,np_yoylm_amt
    ,np_lm_rev_amt
    ,np_plan_fup_lm
    ,np_plan_fup_yoylm
    ,net_roam_rev_amt_1m
    ,net_roam_rev_amt_2m
    ,net_roam_rev_amt_3m
    ,case when skip_flg ='Y' then 0 else ttl_roam_comm_amt end ttl_roam_comm_amt
    ,case when skip_flg ='Y' then 0 else ttl_comm_amt end ttl_comm_amt
    ,comm_rate_flg
    ,comm_rate
    ,comm_pay_method
    ,prv_case_id
    ,prv_np_lm_rev_amt
    ,prv_ld_orig_exp_date
    ,prv_contract_mth
    ,json_rmk
from ${etlvar::TMPDB}.B_BM_CRC_COMM_005A01_T;
commit;

insert into ${etlvar::ADWDB}.BM_CRC_COMM_ROAM_H
(       trx_mth
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
        ,ttl_roam_comm_amt
        ,roam_comm_status
        ,json_rmk
)select
         trx_mth
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
        ,ttl_roam_comm_amt
        ,roam_comm_status
        ,json_rmk
from ${etlvar::TMPDB}.B_BM_CRC_COMM_004_T
where case_id not in (select case_id from ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T where skip_flg='Y');
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

