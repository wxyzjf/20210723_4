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

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_003A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_003B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_003_T');

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
prompt 'Step B_BM_CNC_COMM_003A01_T : [Sub net plan ] ';
--- subr net plan plan tariff ------------
insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003A01_T
(trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk
)
-----------LM Plan tariff
select
    t.trx_mth
    ,t.comm_mth
    ,t.case_id
    ,'NP_PLAN_TARIFF_LM' as val_type
    ,nvl(bs.bill_rate,0)
    ,nvl(s.rate_plan_cd,' ') as val_str
    ,' ' as json_rmk
  from ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T t
left outer join prd_adw.subr_info_hist s
   on t.cust_num = s.cust_num
   and t.subr_num = s.subr_num
   and &comm_mth - 1 between s.start_date and s.end_date
left outer join prd_adw.bill_serv_ref bs
    on s.rate_plan_cd = bs.bill_serv_cd
   and &comm_mth - 1 between bs.eff_start_date and bs.eff_end_date;
commit;

prompt 'Step B_BM_CNC_COMM_003A01_T : [ Rebate plan rebate LM ] ';

insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003A01_T
(trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk
) select
     t.trx_mth
    ,t.comm_mth
    ,t.case_id
    ,'NP_PLAN_REBATE_LM' as val_type
    ,sum(LM_BONUS_REBATE + LM_SERVICE_FEE_REBATE +
     case when LM_CALL_GUARD_IN_DOLLAR > 0
            or ST_PROTECT_BILLCD_TARRIF > 0
            or ST_PROTECT_ENT_BILLCD_TARIFF > 0
            or CALL_GRD_ST_PROT_BILLCD_TARIFF > 0
            or MOBILE_THREAT_PREVENT_BILL_CD <> ' '
            or TRAVELLER_BILL_CD_TARIFF > 0
            or DATA_ROAM_PKT_BILL_CD_TARIFF > 0
     then 0
     else LM_FEATURE_CREDIT_REBATE
     end) as val_amt
     ,' ' as val_str
     ,' ' as json_rmk
 from ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T t
    ,PRD_BIZ_SUMM_VW.VW_LD_RPT_CONT_RENEW_SUMM_MTH  s
 where (t.subr_num = s.subr_num
    or t.split_subr = s.subr_num)
   and trunc(s.trx_month,'MM') = add_months(&comm_mth,-1)
  group by t.trx_mth
    ,t.comm_mth
    ,t.case_id;
commit;


prompt 'Step B_BM_CNC_COMM_003A01_T : [ Net plan FUP ] ';
insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003A01_T
(       trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk
)
 Select
     t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id
    ,'NP_PLAN_FUP_LM' as val_type
    ,nvl(max(p.fup_addon_amt),0) as val_amt
    ,nvl(max(sl.inv_num),' ') as val_str
    ,' ' as json_rmk
  from  ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T t1
       ,prd_adw.subr_ld_hist sl
       ,(select par_grp,par_id,par_val_str as mkt_cd ,par_val_num as fup_addon_amt
           from ${etlvar::ADWDB}.BM_CNC_COMM_PAR_REF
           where par_grp = 'FUP_ADDON'
             and par_id = 'MKT_CD_LST'
             and &trx_mth between par_eff_s_date and par_eff_e_date
         )p
  where t1.subr_num = sl.subr_num
  and t1.cust_num = sl.cust_num
  and &comm_mth - 1 between sl.start_date and sl.end_date
  and &comm_mth - 1 between sl.ld_start_date and sl.ld_expired_date
  and sl.mkt_cd = p.mkt_cd
  group by  t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id;
commit;
prompt 'Step B_BM_CNC_COMM_003A01_T : [ Net plan revenue HS Fund ] ';
insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003A01_T
(trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk
) select
     t.trx_mth
    ,t.comm_mth
    ,t.case_id
    ,'NP_HSF_LM' as val_type
    ,sum(hs.derived_hsf_per_line / hs.hsf_valid_period + hs.hsf_amt_per_mth) as val_amt
     ,' ' as val_str
     ,' ' as json_rmk
 from ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T t
     ,${etlvar::ADWDB}.HS_FUND_FINANCE hs
 where t.ld_inv_num = hs.pos_inv_num
 group by
     t.trx_mth
    ,t.comm_mth
    ,t.case_id;
commit;

prompt 'Step B_BM_CNC_COMM_003A01_T : [ Net plan revenue HS subsidy ] ';
insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003A01_T
(trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk
) select
     t.trx_mth
    ,t.comm_mth
    ,t.case_id
    ,'NP_HS_SUBSIDY_LM' as val_type
    ,sum(s.amort_amt) as val_amt
     ,' ' as val_str
     ,' ' as json_rmk
 from ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T t
     ,prd_adw.HS_AMORT s
 where (t.subr_num = s.subr_num
    or t.split_subr = s.subr_num)
   and trunc(s.trx_month,'MM') = add_months(&comm_mth,-1)
  group by t.trx_mth
    ,t.comm_mth
    ,t.case_id;
commit;
--prompt 'Step B_BM_CNC_COMM_003A01_T : [ Net plan revenue Company subsidy ] ';
--insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003A01_T
--(trx_mth
--        ,comm_mth
--        ,case_id
--        ,val_Type
--        ,val_amt
--        ,val_str
--        ,json_rmk
--) select
--     t.trx_mth
--    ,t.comm_mth
--    ,t.case_id
--    ,'NP_COMPANY_SUBSIDY_LM' as val_type
--    ,sum(s.line_amt) as val_amt
--     ,' ' as val_str
--     ,' ' as json_rmk
-- from ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T t
--     ,prd_adw.inv_detail s
-- where (t.subr_num = s.subr_num
--    or t.split_subr = s.subr_num)
--   and s.inv_date between add_months(&comm_mth,-1) and &comm_mth-1
--   and s.type_name like 'DCOSA%'
--   and s.inv_line_type_cd ='ADJ'
--  group by t.trx_mth
--    ,t.comm_mth
--    ,t.case_id;
--commit;


prompt 'Step B_BM_CNC_COMM_003A01_T : [ Net plan revenue Buyout subsidy ] ';
insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003A01_T
(trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk
) select
     t.trx_mth
    ,t.comm_mth
    ,t.case_id
    ,'NP_BUYOUT_COST_LM' as val_type
    ,max(case when substr(s.ld_cd,6,1)='F' 
            then to_number(substr(s.ld_cd,7))
          when substr(s.ld_cd,6,1)='M' 
            then to_number(substr(s.ld_cd,4,2)) * to_number(substr(s.ld_cd,7))
          else 0  
      end) keep (dense_rank first order by 
      case when substr(s.ld_cd,6,1)='F' 
            then to_number(substr(s.ld_cd,7))
          when substr(s.ld_cd,6,1)='M' 
            then to_number(substr(s.ld_cd,4,2)) * to_number(substr(s.ld_cd,7))
          else 0  
      end desc )   val_amt
    ,' ' as val_str
    ,' ' as json_rmk
 from ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T t
     ,prd_adw.subr_ld_hist s 
 where (t.subr_num = s.subr_num
    or t.split_subr = s.subr_num)
    and t.cust_num = s.cust_num
   and &trx_e_date between s.start_date and s.end_date
   and &trx_e_date between s.ld_start_date and s.ld_expired_date
   and s.waived_flg<>'Y'
   and s.void_flg<>'Y'
   and s.billed_flg<>'Y'
   and s.ld_cd like 'LDY%'
   and s.ld_start_date >= t.subr_sw_on_date
   group by  t.trx_mth
    ,t.comm_mth
    ,t.case_id;
commit;
   


prompt 'Step B_BM_CNC_COMM_003A01_T : [ Net plan roam of last month ] ';

insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003A01_T
(
trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk
) select
     t.trx_mth
    ,t.comm_mth
    ,t.case_id
    ,'NP_ROAM_REV_LM'  
    ---- use LM
    ,nvl(sum(decode(pr.trx_month, to_char(add_months(t.comm_mth,-1),'yyyymm'), pr.NET_OUT_ROAM_REV,0)),0) NET_ROAM_REV_AMT_LM
    ,' ' as val_str
    ,' ' as json_rmk
    from ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T t
    left outer join PRD_BIZ_SUMM_VW.VW_PRF_SUBR_INFO pr
        on t.subr_num = pr.subr_num
        and pr.trx_month between to_char(t.trx_mth,'yyyymm') and to_char(add_months(t.trx_mth ,6),'yyyymm')
    group by  t.trx_mth
    ,t.comm_mth
    ,t.case_id;
commit;


insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003B01_T
(
     case_id
    ,np_plan_tariff_lm
    ,np_plan_rebate_lm
    ,np_hs_subsidy_lm
    ,np_company_rebate_lm
    ,np_plan_fup_lm
    ,np_hsf_lm
    ,np_buyout_cost_lm
    ,np_lm_amt
    ,np_roam_rev_lm
    ,json_rmk
)
select 
    case_id
    ,sum(decode(val_type,'NP_PLAN_TARIFF_LM',val_amt,0))
    ,sum(decode(val_type,'NP_PLAN_REBATE_LM',val_amt,0))
    ,sum(decode(val_type,'NP_HS_SUBSIDY_LM',val_amt,0))
    ,sum(decode(val_type,'NP_COMPANY_REBATE_LM',val_amt,0))
    ,sum(decode(val_type,'NP_PLAN_FUP_LM',val_amt,0))
    ,sum(decode(val_type,'NP_HSF_LM',val_amt,0))
    ,sum(decode(val_type,'NP_BUYOUT_COST_LM',val_amt,0))
    ,greatest(sum(decode(val_type,'NP_PLAN_TARIFF_LM',val_amt,0)) 
        + sum(decode(val_type,'NP_HSF_LM',val_amt,0))
        + sum(decode(val_type,'NP_PLAN_REBATE_LM',val_amt,0)) 
        + sum(decode(val_type,'NP_PLAN_FUP_LM',val_amt,0)) 
        - sum(decode(val_type,'NP_HS_SUBSIDY_LM',val_amt,0))
        + sum(decode(val_type,'NP_COMPANY_REBATE_LM',val_amt,0))
        + sum(decode(val_type,'NP_BUYOUT_COST_LM',val_amt,0)),0)
     as  np_lm_amt 
    ,sum(decode(val_type,'NP_ROAM_REV_LM',val_amt,0))
    ,',"ORIG_NP_LM_AMT":"'||to_char(
                sum(decode(val_type,'NP_PLAN_TARIFF_LM',val_amt,0))
        + sum(decode(val_type,'NP_HSF_LM',val_amt,0))
        + sum(decode(val_type,'NP_PLAN_REBATE_LM',val_amt,0))
        + sum(decode(val_type,'NP_PLAN_FUP_LM',val_amt,0))
        - sum(decode(val_type,'NP_HS_SUBSIDY_LM',val_amt,0))
        + sum(decode(val_type,'NP_COMPANY_REBATE_LM',val_amt,0))
        + sum(decode(val_type,'NP_BUYOUT_COST_LM',val_amt,0))) ||'"' as json_rmk
from ${etlvar::TMPDB}.B_BM_CNC_COMM_003A01_T t
group by case_id ;
commit;

insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_003_T
(
     case_id
    ,np_plan_tariff_lm
    ,np_plan_rebate_lm
    ,np_hs_subsidy_lm
    ,np_company_rebate_lm
    ,np_plan_fup_lm
    ,np_hsf_lm
    ,np_buyout_cost_lm
    ,np_lm_amt
    ,np_roam_rev_lm
    ,json_rmk
)
select 
 case_id
    ,np_plan_tariff_lm
    ,np_plan_rebate_lm
    ,np_hs_subsidy_lm
    ,np_company_rebate_lm
    ,np_plan_fup_lm
    ,np_hsf_lm
    ,np_buyout_cost_lm
    ,np_lm_amt
    ,np_roam_rev_lm
    ,json_rmk
from  ${etlvar::TMPDB}.B_BM_CNC_COMM_003B01_T;
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

