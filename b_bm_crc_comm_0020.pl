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

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_002A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_002A02_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_002A03_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_002A04_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_002B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_002_T');

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

---------------------Bulk Roaming Data Plan -----------------------------------------------------------------------------------
prompt 'Step B_BM_CRC_COMM_001A01_T : [Filter Bulk roaming case  ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_002A01_T
(
    trx_mth
   ,comm_mth
   ,case_id
   ,case_type
   ,ld_inv_num
   ,ld_cust_num
   ,ld_subr_num
   ,split_subr
   ,ld_cd
   ,rate_plan_cd
   ,contract_mth
)
select
    t.trx_mth
   ,t.comm_mth
   ,t.case_id
   ,'CASE_BR' as case_type
   ,t.ld_inv_num
   ,t.ld_cust_num
   ,t.ld_subr_num
   ,split_subr
   ,t.ld_cd
   ,t.rate_plan_cd
   ,substr(t.ld_cd,4,2) as contract_mth   
from ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T t    
where 
---##diable skip 
--    t.skip_flg<>'Y'
--and 
t.case_type = ' '
and t.rate_plan_cd in (
    SELECT 
           r.BILL_SERV_CD         
     FROM PRD_ADW.BM_COMM_BILL_CD_REF r
     where r.trx_month = &trx_s_date
     and r.subcategory='H4'
);
commit;

prompt 'Step B_BM_CRC_COMM_001A02_T : [Preparing the profitability out_roam_sub_data_chrg ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_002A02_T
(
        case_id
        ,ttl_out_roam_data
        ,spa_out_roam_data
)
---------- out roam data rev from profitabily------
Select t.case_id
          ,sum(p.t_out_roam_subs_data_chrg) ttl_out_roam_data
          ,sum(case when p.subr_num like '448%' then p.t_out_roam_subs_data_chrg else 0 end) as spa_out_roam_data
from ${etlvar::TMPDB}.B_BM_CRC_COMM_002A01_T t
    ,(select /*+ materialize */
                 pr.trx_month
                ,pr.cust_num
                ,pr.subr_num
                ,sum(pr.out_roam_subs_data_chrg) as t_out_roam_subs_data_chrg
           from prd_adw.prf_subr_info pr 
           where pr.trx_month = to_number(to_char(trunc(&comm_mth - 1,'MM') ,'yyyymm'))
           group by pr.trx_month ,pr.cust_num,pr.subr_num
         )p
where p.trx_month = to_number(to_char(trunc(&comm_mth - 1,'MM') ,'yyyymm'))
       and (t.ld_subr_num = p.subr_num or t.split_subr = p.subr_num)
       and t.ld_cust_num = p.cust_num
       and p.subr_num<> ' '
group by t.case_id;
commit;

prompt 'Step B_BM_CRC_COMM_001A02_T : [Preparing the crbk ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_002A03_T
(
        case_id
        ,ttl_crbk_amt
        ,spa_crbk_amt
)
select t.case_id
          ,sum(ct.crbk_amt) ttl_crbk_amt
          ,sum(case when ct.subr_num like '448%' then ct.crbk_amt else 0 end ) as spa_crbk_amt
from  ${etlvar::TMPDB}.B_BM_CRC_COMM_002A01_T t
     ,( Select   /*+ materialize */ cr.cust_num
              ,cr.subr_num
              ,sum(cr.crbk_amt) crbk_amt
       FROM PRD_ADW.CRBK_REMAIN cr
       where cr.extraction_date between add_months(&comm_mth,-1) and &comm_mth -1
       group by cr.cust_num,cr.subr_num
      ) ct
where (t.ld_subr_num = ct.subr_num or t.split_subr = ct.subr_num)
        and t.ld_cust_num = ct.cust_num
        and ct.subr_num<> ' '
group by t.case_id;
commit;

prompt 'Step B_BM_CRC_COMM_002A04_T : [Map previous case for pro rata ] ';

insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_002A04_T
(
  case_id
 ,prv_case_id
 ,prv_case_src
 ,prv_ld_start_date
 ,prv_ld_orig_exp_date
 ,prv_np_lm_amt
 ,prv_contract_mth
 ,prv_np_lm_rev_amt
) Select t.case_id
      ,max(rh.case_id) keep(dense_rank first order by rh.comm_mth desc) as prv_case_id
----Reserver PRV_NEWACTV -----
      ,'PRV_RETENT' as prv_case_src
      ,max(rh.ld_start_date)keep(dense_rank first order by rh.comm_mth desc)  as prv_ld_start_date
      ,max(rh.ld_orig_exp_date) keep(dense_rank first order by rh.comm_mth desc)  as prv_ld_orig_exp_date
      ,max(rh.np_lm_amt)keep(dense_rank first order by rh.comm_mth desc)  as prv_np_lm_amt
      ,max(rh.contract_mth) keep(dense_rank first order by rh.comm_mth desc)  as prv_contract_mth
      ,max(rh.np_lm_rev_amt) keep(dense_rank first order by rh.comm_mth desc)  as prv_np_lm_rev_amt
  from ${etlvar::TMPDB}.B_BM_CRC_COMM_002A01_T t
      ,${etlvar::TMPDB}.B_BM_CRC_COMM_001_T t1
----reserver for BM_CNC_COMM_H ------
      ,(select comm_mth,lm_subr_num,lm_cust_num,case_id,ld_start_date,ld_orig_exp_date,np_lm_amt,contract_mth,np_lm_rev_amt 
        from ${etlvar::ADWDB}.BM_CRC_COMM_H where comm_mth < &comm_mth) rh
where t.case_id = t1.case_id
  and t1.lm_subr_num = rh.lm_subr_num
  and t1.lm_cust_num = rh.lm_cust_num
  and rh.comm_mth < t1.comm_mth
  and rh.ld_orig_exp_date >= t1.ld_start_date
  group by t.case_id;


prompt 'Step B_BM_CRC_COMM_002B01_T : [Map roam data charge and credit back rebate ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_002B01_T 
(   trx_mth
   ,comm_mth
   ,case_id
   ,case_type
   ,ld_inv_num
   ,ld_cust_num
   ,ld_subr_num
   ,ld_cd
   ,rate_plan_cd
   ,contract_mth
   ,lm_ob_roam_data_rev_amt
   ,lm_rebate_amt   
   ,comm_rate
   ,comm_pay_method
   ,ttl_comm_amt
   ,np_lm_amt
   ,np_lm_rev_amt
   ,ld_start_date
   ,prv_case_id
   ,prv_case_src
   ,prv_ld_start_date
   ,prv_ld_orig_exp_date
   ,prv_np_lm_amt
   ,prv_contract_mth
   ,prv_np_lm_rev_amt
   ,json_rmk)
select
    t.trx_mth
   ,t.comm_mth
   ,t.case_id
   ,t.case_type
   ,t.ld_inv_num
   ,t.ld_cust_num
   ,t.ld_subr_num
   ,t.ld_cd
   ,t.rate_plan_cd
   ,t.contract_mth
   ,nvl(ro.ttl_out_roam_data,0)as  lm_ob_roam_data_rev_amt
   ,nvl(crr.ttl_crbk_amt,0) as lm_rebate_amt   
   ,pp.comm_rate  as  comm_rate
   ,'ONEOFF' as comm_pay_method
   ,0 as ttl_comm_amt
   ,0 as np_lm_amt
   ,0 as np_lm_rev_amt
   ,nvl(t1.ld_start_date,date '1900-01-01')
   ,nvl(pr.prv_case_id,' ')
   ,nvl(pr.prv_case_src,' ')
   ,nvl(pr.prv_ld_start_date,date '2999-12-31')
   ,nvl(pr.prv_ld_orig_exp_date,date '2999-12-31')
   ,nvl(pr.prv_np_lm_amt,0)
   ,nvl(pr.prv_contract_mth,0)
   ,nvl(pr.prv_np_lm_rev_amt,0)
   ,case when ro.spa_out_roam_data > 0 or crr.spa_crbk_amt >0 then           
      ',"CASE_BR_SPLITSUBR_AMT":"out_roam_data-'||nvl(ro.spa_out_roam_data,0)||';crbak-'||nvl(crr.spa_crbk_amt,0)||'"' 
    else ',NA' end as json_rmk
from ${etlvar::TMPDB}.B_BM_CRC_COMM_002A01_T t
--------------Join profitability ------------
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_002A02_T ro 
        on t.case_id = ro.case_id    
--------------Join creditback ------------
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_002A03_T crr
on t.case_id = crr.case_id
left outer join (
-------------join every row with commission rate 
        select par_val_num  as comm_rate
        from ${etlvar::ADWDB}.BM_CRC_COMM_PAR_REF bpr 
        where bpr.PAR_GRP='CASE_BR_RATE' 
          and bpr.PAR_ID='COMM_RATE'
          and &comm_mth between bpr.par_eff_s_date and bpr.par_eff_e_date
)pp
on 1=1 
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_002A04_T pr
        on t.case_id = pr.case_id 
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T t1
        on t.case_id = t1.case_id;
commit;

----Calculate commission 
update ${etlvar::TMPDB}.B_BM_CRC_COMM_002B01_T t
     set ttl_comm_amt =  nvl((lm_ob_roam_data_rev_amt - lm_rebate_amt) * contract_mth * comm_rate,0)
        ,np_lm_amt = nvl(lm_ob_roam_data_rev_amt - lm_rebate_amt,0) 
        ,np_lm_rev_amt = greatest((nvl(lm_ob_roam_data_rev_amt - lm_rebate_amt,0) * t.contract_mth )
         - case when nvl(t.prv_case_id,' ') <> ' ' and t.prv_ld_orig_exp_date > t.ld_start_date
           then nvl(t.prv_np_lm_rev_amt,0)/t.prv_contract_mth * round(months_between(t.prv_ld_orig_exp_date ,t.ld_start_date),0)
           else 0
         end,0);
commit;
-----As we use valid ld all cases should be valid
-----Exceptional checking the the case with subr_sw_off_date in lm -----
--update ${etlvar::TMPDB}.B_BM_CRC_COMM_002B01_T t
--    set t.ttl_comm_amt = 0 
--      ,t.json_rmk= t.json_rmk||',"CASE_BR_SKIP_SW_OFF":"Y"'
--where t.case_id in (
--      select r.case_id 
--      from ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T r
--      where r.subr_sw_off_date <= &comm_mth - 1 
--);
--commit;



---- Insert into 002_T

insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_002_T
(  trx_mth
   ,comm_mth
   ,case_id
   ,case_type
   ,ld_inv_num
   ,ld_cust_num
   ,ld_subr_num
   ,ld_cd
   ,rate_plan_cd
   ,contract_mth
   ,lm_ob_roam_data_rev_amt
   ,lm_rebate_amt
   ,ttl_comm_amt
   ,comm_rate
   ,np_lm_amt
   ,np_lm_rev_amt
   ,comm_pay_method
   ,ld_start_date
   ,prv_case_id
   ,prv_case_src
   ,prv_ld_start_date
   ,prv_ld_orig_exp_date
   ,prv_np_lm_amt
   ,prv_contract_mth
   ,prv_np_lm_rev_amt
   ,json_rmk )
select
   trx_mth
   ,comm_mth
   ,case_id
   ,case_type
   ,ld_inv_num
   ,ld_cust_num
   ,ld_subr_num
   ,ld_cd
   ,rate_plan_cd
   ,contract_mth
   ,lm_ob_roam_data_rev_amt
   ,lm_rebate_amt
   ,ttl_comm_amt
   ,comm_rate
   ,np_lm_amt
   ,np_lm_rev_amt
   ,comm_pay_method
   ,ld_start_date
   ,prv_case_id
   ,prv_case_src
   ,prv_ld_start_date
   ,prv_ld_orig_exp_date
   ,prv_np_lm_amt
   ,prv_contract_mth
   ,prv_np_lm_rev_amt
   ,json_rmk
from  ${etlvar::TMPDB}.B_BM_CRC_COMM_002B01_T t;
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

