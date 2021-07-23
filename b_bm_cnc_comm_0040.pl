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

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_004A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_004A02_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_004_T');

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
prompt 'Step B_BM_CNC_COMM_004A01_T : [Preapre the mapping figure ] ';

insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_004A01_T
(
   case_id
  ,case_type
  ,dealer_cd
  ,cust_id_type
  ,ld_inv_num
  ,ld_cd
  ,ld_mkt_cd
  ,ld_start_date
  ,ld_expired_date
  ,ld_inv_date
  ,ld_contract_period
  ,split_acct
  ,split_subr
  ,rate_plan_cd
  ,map_plan_type
  ,np_lm_amt
  ,np_roam_rev_lm
  ,np_plan_tariff_lm
)
select
   t1.case_id
  ,t1.case_type
  ,t1.dealer_cd
  ,t1.cust_id_type
  ,t1.ld_inv_num
  ,t1.ld_cd
  ,t1.ld_mkt_cd
  ,t1.ld_start_date
  ,t1.ld_expired_date
  ,t1.ld_inv_date
  ,t1.ld_contract_period
  ,t1.split_acct
  ,t1.split_subr
  ,t1.rate_plan_cd
  ,nvl(t2.map_plan_type,'OTHERS')
  ,nvl(t3.np_lm_amt,0)
  ,nvl(t3.np_roam_rev_lm,0)
  ,nvl(t3.np_plan_tariff_lm,0)
from ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T  t1
left outer join  ${etlvar::TMPDB}.B_BM_CNC_COMM_002_T  t2
        on t1.rate_plan_cd  = t2.rate_plan_cd
left outer join  ${etlvar::TMPDB}.B_BM_CNC_COMM_003_T  t3
        on t1.case_id = t3.case_id
where t1.case_type not in ('CASE_ES');
commit;


prompt 'Step B_BM_CNC_COMM_004A02_T : [Calculate the mapping figure ] ';

insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_004A02_T
(
     case_id
    ,tcv_achieve_ptg
    ,ttl_tcv_salesman
    ,np_plan_tariff_tier
    ,comm_rate
    ,json_rmk
)
select 
     t.case_id    
    ,round(tcv_sales.ttl_tcv_salesman/tgt_r.tgt_amt,2) tcv_achieve_ptg
    ,tcv_sales.ttl_tcv_salesman
    ,case when t.cust_id_type like 'CUSTID_BR' and np_r.tier is not null then np_r.tier 
          else 'TIER1'
     end np_plan_tariff_tier           
    ,0 as comm_rate
    ,',"NP_TARIFF_TIER":"'||np_r.map_plan_type||'-'||np_r.from_amt||'-'||np_r.to_amt||'"' json_rmk
  from ${etlvar::TMPDB}.B_BM_CNC_COMM_004A01_T t  
left outer join (
        select   r.par_grp
        ,r.par_id as map_plan_type
        ,par_val_str as tier
        ,par_val_num as from_amt
        ,par_val_num_2 as to_amt
      from ${etlvar::ADWDB}.BM_CNC_COMM_PAR_REF r 
     where r.par_grp ='NP_TARIFF_TIER'
     and &comm_mth between r.par_eff_s_date and r.par_eff_e_date
)np_r
    on t.map_plan_type = np_r.map_plan_type
    --and round(t.np_plan_tariff_lm,0) between np_r.from_amt and np_r.to_amt
    and round(t.np_lm_amt,0) between np_r.from_amt and np_r.to_amt
left outer join( 
    Select st.dealer_cd
      ,sum((st.np_lm_amt + np_roam_rev_lm) * st.ld_contract_period) as  ttl_tcv_salesman
  from ${etlvar::TMPDB}.B_BM_CNC_COMM_004A01_T st       
  group by st.dealer_cd
)tcv_sales 
    on t.dealer_cd = tcv_sales.dealer_cd
left outer join (
    Select r.par_val_num as tgt_amt from ${etlvar::ADWDB}.BM_CNC_COMM_PAR_REF r where par_grp ='MTHLY_NET_PRJ_REV_TARGET'
)tgt_r  
    on 1=1;
commit;

update ${etlvar::TMPDB}.B_BM_CNC_COMM_004A02_T t
    set comm_rate = (
            select max(r.par_val_num) as comm_rate
             from ${etlvar::ADWDB}.BM_CNC_COMM_PAR_REF r
             where r.par_grp ='ACTV_COMM_RATE'
             and &comm_mth between r.par_eff_s_date and r.par_eff_e_date
             and r.par_val_str= t.np_plan_tariff_tier
             and case when t.tcv_achieve_ptg * 100 < 100 then 'TCV_PTG_LV1'
                  when  t.tcv_achieve_ptg * 100 >= 100 and t.tcv_achieve_ptg * 100 <= 129.9 then 'TCV_PTG_LV2'
                  when  t.tcv_achieve_ptg * 100 > 129.9  then 'TCV_PTG_LV3'
                else 'TCV_PTG_LV1'
             end = r.PAR_ID )
      ,json_rmk = t.json_rmk||',"ACTV_COMM_RATE":"'||case when t.tcv_achieve_ptg * 100 < 100 then 'TCV_PTG_LV1'
                  when  t.tcv_achieve_ptg * 100 >= 100 and t.tcv_achieve_ptg * 100 <= 129.9 then 'TCV_PTG_LV2'
                  when  t.tcv_achieve_ptg * 100 > 129.9  then 'TCV_PTG_LV3'
                else 'TCV_PTG_LV1'
             end||'"';
commit;

insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_004_T
(
   case_id
  ,case_type
  ,dealer_cd
  ,ld_inv_num
  ,ld_cd
  ,ld_mkt_cd
  ,ld_start_date
  ,ld_expired_date
  ,ld_inv_date
  ,ld_contract_period
  ,split_acct
  ,split_subr
  ,map_plan_type
  ,np_lm_amt
  ,np_roam_rev_lm
  ,np_plan_tariff_lm
  ,rate_plan_cd
  ,cust_id_type
  ,tcv_achieve_ptg
  ,ttl_tcv_salesman
  ,np_plan_tariff_tier
  ,comm_rate
  ,base_comm
  ,json_rmk
)
select
   t.case_id
  ,t.case_type
  ,t.dealer_cd
  ,t.ld_inv_num
  ,t.ld_cd
  ,t.ld_mkt_cd
  ,t.ld_start_date
  ,t.ld_expired_date
  ,t.ld_inv_date
  ,t.ld_contract_period
  ,t.split_acct
  ,t.split_subr
  ,t.map_plan_type
  ,t.np_lm_amt
  ,t.np_roam_rev_lm
  ,t.np_plan_tariff_lm
  ,t.rate_plan_cd
  ,t.cust_id_type
  ,nvl(t2.tcv_achieve_ptg,0)
  ,nvl(t2.ttl_tcv_salesman,0)
  ,nvl(t2.np_plan_tariff_tier,' ')
  ,nvl(t2.comm_rate,0)
  ,t.np_lm_amt * t.ld_contract_period * nvl(t2.comm_rate,0) as base_comm
  ,nvl(t2.json_rmk,' ')||',"COMM_TIER_INFO":"'||'tcv_ptg-'||round(t2.tcv_achieve_ptg,2)*100 ||';ttl_tcv_sales-'||t2.ttl_tcv_salesman||';np_tariff_tier-'||t2.np_plan_tariff_tier||'"'
from  ${etlvar::TMPDB}.B_BM_CNC_COMM_004A01_T t
left outer join  ${etlvar::TMPDB}.B_BM_CNC_COMM_004A02_T t2
  on t.case_id = t2.case_id;
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

