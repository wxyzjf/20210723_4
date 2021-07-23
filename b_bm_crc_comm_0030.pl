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

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_003A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_003B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_003C01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_003D01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_003_T');
---- comm paid at 2021-01
---- last mth of comm paid snapshot at 2020-12-31

set define on;
define tx_date = trunc(to_date('$etlvar::TXDATE','YYYY-MM-DD'),'MM');
define comm_mth = add_months(&tx_date,-1);
define trx_mth = add_months(&comm_mth,-4);
define trx_s_date = add_months(&comm_mth,-4);
define trx_e_date = add_months(&comm_mth,-3)-1;

---------------------Basic Telelcom -----------------------------------------------------------------------------------
prompt 'Step B_BM_CRC_COMM_003A01_T : [Filter Bulk roaming case  ] ';
 insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T
(
    trx_mth
   ,comm_mth
   ,case_id
   ,case_type
   ,ld_inv_num
   ,ld_mkt_cd
   ,ld_cust_num
   ,ld_subr_num
   ,lm_cust_num
   ,lm_subr_num
   ,split_subr
   ,ld_cd
   ,rate_plan_cd   
   ,idbr_prefix 
   ,np_yoy_cust_num
   ,np_yoy_offset
   ,yoy_offset
)
select
    t.trx_mth
   ,t.comm_mth
   ,t.case_id
   ,case when nvl(max(g.D_5G_FLG),'N') ='Y' then 'CASE_BT_5G' else 'CASE_BT_NON5G' end as case_type
   ,t.ld_inv_num
   ,t.ld_mkt_cd
   ,t.ld_cust_num
   ,t.ld_subr_num
   ,t.lm_cust_num
   ,t.lm_subr_num
   ,t.split_subr
   ,t.ld_cd
   ,t.rate_plan_cd   
   ,t.idbr_prefix 
   ,yoy.np_yoy_cust_num
   ,yoy.np_yoy_offset
   ,yoy.yoy_offset
from ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T t
--left outer join prd_biz_summ_vw.vw_bill_servs_5g_flg g
-- on t.lm_subr_num = g.subr_num
left outer join (
    SELECT  cust_num,
            subr_num,
            MAX (
               CASE
                  WHEN    (    BM_category = 'B'
                           AND BM_subcategory IN ('7', '8', '9', '10'))
                       OR (BM_category = 'T' AND BM_subcategory IN ('29'))
                  THEN
                     'Y'
                  ELSE
                     'N'
               END)
               D_5G_FLG
       FROM  PRD_BIZ_SUMM_VW.VW_BILL_SERVS 
      WHERE &comm_mth - 1 between bill_start_date and bill_end_date 
        and  (    BM_category = 'B'
                           AND BM_subcategory IN ('7', '8', '9', '10'))
                       OR (BM_category = 'T' AND BM_subcategory IN ('29'))                
   GROUP BY cust_num,subr_num
)g
 on t.lm_subr_num = g.subr_num
--------- combine the yoy parameter and np yoy cust num into table ----
left outer join (
   select 
         tt.case_id    
        ,nvl(max(cy.cust_num),' ') as np_yoy_cust_num
        ,max(yh.np_yoy_offset) as np_yoy_offset
        ,max(yo.yoy_offset) as yoy_offset           
    from ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T tt
    left outer join (
            Select par_val_num np_yoy_offset
              from ${etlvar::ADWDB}.BM_CRC_COMM_PAR_REF
             where par_grp like 'NP_YOY_MTH_OFFSET'
               and &trx_mth between par_eff_s_date and par_eff_e_date
    ) yh on 1=1
    left outer join (
            Select par_val_num yoy_offset
              from ${etlvar::ADWDB}.BM_CRC_COMM_PAR_REF
             where par_grp like 'YOY_MTH_OFFSET'
               and &trx_mth between par_eff_s_date and par_eff_e_date
    ) yo on 1=1
    left outer join prd_adw.subr_info_hist sy
            on tt.lm_subr_num = sy.subr_num
           and add_months(&comm_mth,-yh.np_yoy_offset) between  sy.start_date and sy.end_date
           and sy.subr_stat_cd in ('OK','SU')
    left outer join prd_adw.cust_info_hist cy
            on sy.cust_num = cy.cust_num
           and add_months(&comm_mth,- yh.np_yoy_offset) between  cy.start_date and cy.end_date
           and cy.cust_stat_cd in ('OK','SU')
           and cy.hkid_br_prefix = tt.idbr_prefix
   group by tt.case_id
) yoy
        on t.case_id = yoy.case_id
where
--#disable skip --#
--    t.skip_flg<>'Y'
--and 
t.case_type = ' '
and t.case_id not in ( SELECT r.case_id FROM ${etlvar::TMPDB}.B_BM_CRC_COMM_002_T r) 
group by 
    t.trx_mth
   ,t.comm_mth
   ,t.case_id
   ,t.ld_inv_num
   ,t.ld_cust_num
   ,t.ld_subr_num
   ,t.ld_mkt_cd
   ,t.lm_cust_num
   ,t.lm_subr_num
   ,t.split_subr
   ,t.ld_cd
   ,t.rate_plan_cd
   ,t.idbr_prefix 
   ,yoy.np_yoy_cust_num
   ,yoy.np_yoy_offset
   ,yoy.yoy_offset;
commit;

-----------------------------Part a-----------------------------------
prompt 'Step B_BM_CRC_COMM_003B01_T : [KPI ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T
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
    ,case when abs(months_between(t.comm_mth,k.trx_month)) = 1 then 'KPI_LM'    
          when abs(months_between(t.comm_mth,k.trx_month)) = 2 then 'KPI_L2M'
          when abs(months_between(t.comm_mth,k.trx_month)) = 3 then 'KPI_L3M'
          when abs(months_between(t.comm_mth,k.trx_month)-t.yoy_offset+1) = 1 then 'KPI_YOYLM'
          when abs(months_between(t.comm_mth,k.trx_month)-t.yoy_offset+1) = 2 then 'KPI_YOYL2M'
          when abs(months_between(t.comm_mth,k.trx_month)-t.yoy_offset+1) = 3 then 'KPI_YOYL3M'
     else 'KPI_NONE'          
     end val_type
    ,sum(   k.PLAN_4G3G_REV
            +k.VOICE_ONLY_PLAN_REV
            +k.BB_PLAN_REV
            +k.MBB_PLAN_REV
            +k.M2M_PLAN_REV
            +k.OTHER_MOB_REV
            +k.DATA_ROAM_THEREAFTER_CHG_REV
            +k.Net_Roam_SMS_Rev
            +k.LOCAL_VOICE_REV
            +k.LOCAL_DATA_REV
            +k.VOICE_ROAM_REV
            +k.SMART_IDD_REV
            +k.IDD1638_REV
            +k.Subs_Serv_Rev
            +k.Out_Roam_Other_Rev
            +k.LOCAL_SMS_REV
            +k.INTL_SMS_REV
            +k.OTHER_REV
            +k.TOTAL_ADJ) as val_amt
    ,' '
    ,' ' as json_rmk
from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
    ,prd_adw.KPI_ALL_CA_REV k
where  k.hkid_br_prefix = t.idbr_prefix
  and (k.trx_month between add_months(&comm_mth,-3) and add_months(&comm_mth,-1)
        or k.trx_month between add_months(&comm_mth,-3 -t.yoy_offset +1) and add_months(&comm_mth, -1 -t.yoy_offset+1)
        )
group by  t.trx_mth
    ,t.comm_mth 
    ,t.case_id
    ,case when abs(months_between(t.comm_mth,k.trx_month)) = 1 then 'KPI_LM'    
          when abs(months_between(t.comm_mth,k.trx_month)) = 2 then 'KPI_L2M'
          when abs(months_between(t.comm_mth,k.trx_month)) = 3 then 'KPI_L3M'
          when abs(months_between(t.comm_mth,k.trx_month)-t.yoy_offset + 1) = 1 then 'KPI_YOYLM'
          when abs(months_between(t.comm_mth,k.trx_month)-t.yoy_offset + 1) = 2 then 'KPI_YOYL2M'
          when abs(months_between(t.comm_mth,k.trx_month)-t.yoy_offset + 1) = 3 then 'KPI_YOYL3M'
     else 'KPI_NONE'          
     end;
commit;

prompt 'Step B_BM_CRC_COMM_003B01_T : [HSFUND ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T
(trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk
)Select  
     t.trx_mth
    ,t.comm_mth 
    ,t.case_id
    ,case when abs(months_between(t.comm_mth,hsf.trx_month)) = 1 then 'HSFUND_LM'    
          when abs(months_between(t.comm_mth,hsf.trx_month)) = 2 then 'HSFUND_L2M'
          when abs(months_between(t.comm_mth,hsf.trx_month)) = 3 then 'HSFUND_L3M'
          when abs(months_between(t.comm_mth,hsf.trx_month)-t.yoy_offset + 1) = 1 then 'HSFUND_YOYLM'
          when abs(months_between(t.comm_mth,hsf.trx_month)-t.yoy_offset + 1) = 2 then 'HSFUND_YOYL2M'
          when abs(months_between(t.comm_mth,hsf.trx_month)-t.yoy_offset + 1) = 3 then 'HSFUND_YOYL3M'
     else 'HSFUND_NONE'          
     end val_type     
    ,sum((HSF_ENTITLE_AMT -(hsf.hsf_transfer_amount_from + hsf.hsf_transfer_amount_to)
            )/ ceil(months_between(hsf.expiry_date,hsf.start_date)))
    ,' ' as val_str
    ,' ' as json_rmk                             
from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t 
    ,prd_biz_summ_vw.VW_HSF_MTH_MONITOR_SUMM hsf
where hsf.br = t.idbr_prefix
  and (hsf.trx_month between add_months(&comm_mth,-3) and add_months(&comm_mth,-1)
    or hsf.trx_month between add_months(&comm_mth,-3-t.yoy_offset+1) and add_months(&comm_mth,-1-t.yoy_offset+1)
  )
group by 
        t.trx_mth
        ,t.comm_mth
        ,t.case_id
        ,case when abs(months_between(t.comm_mth,hsf.trx_month)) = 1 then 'HSFUND_LM'
          when abs(months_between(t.comm_mth,hsf.trx_month)) = 2 then 'HSFUND_L2M'
          when abs(months_between(t.comm_mth,hsf.trx_month)) = 3 then 'HSFUND_L3M'
          when abs(months_between(t.comm_mth,hsf.trx_month)-t.yoy_offset + 1) = 1 then 'HSFUND_YOYLM'
          when abs(months_between(t.comm_mth,hsf.trx_month)-t.yoy_offset + 1) = 2 then 'HSFUND_YOYL2M'
          when abs(months_between(t.comm_mth,hsf.trx_month)-t.yoy_offset + 1) = 3 then 'HSFUND_YOYL3M'
         else 'HSFUND_NONE'
        end;
commit;

prompt 'Step B_BM_CRC_COMM_003B01_T : [HS Subsidy ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T
(trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk
)
Select 
       t.trx_mth
      ,t.comm_mth
      ,t.case_id
      ,case when abs(months_between(t.comm_mth,v.trx_month)) = 1 then 'HSSUBSIDY_LM'
          when abs(months_between(t.comm_mth,v.trx_month)) = 2 then 'HSSUBSIDY_L2M'
          when abs(months_between(t.comm_mth,v.trx_month)) = 3 then 'HSSUBSIDY_L3M'
          when abs(months_between(t.comm_mth,v.trx_month)-t.yoy_offset + 1) = 1 then 'HSSUBSIDY_YOYLM'
          when abs(months_between(t.comm_mth,v.trx_month)-t.yoy_offset + 1) = 2 then 'HSSUBSIDY_YOYL2M'
          when abs(months_between(t.comm_mth,v.trx_month)-t.yoy_offset + 1) = 3 then 'HSSUBSIDY_YOYL3M'
         else 'HSSUBSIDY_NONE'
        end as val_type     
      ,sum(v.amort_amt) val_amt
      ,' ' val_str 
      ,' ' as json_rmk
  from PRD_BIZ_SUMM_VW.VW_HS_AMORT v
      ,prd_adw.cust_info_hist c
      ,${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
where
    v.cust_num = c.cust_num
and add_months(v.trx_month,1)-1 between c.start_date and c.end_date 
and t.idbr_prefix = c.hkid_br_prefix
and (v.trx_month between add_months(&comm_mth,-3) and add_months(&comm_mth,-1)
     or v.trx_month between add_months(&comm_mth,-3-t.yoy_offset+1) and add_months(&comm_mth,-1-t.yoy_offset+1))     
group by   t.trx_mth
        ,t.comm_mth
        ,t.case_id
        ,case when abs(months_between(t.comm_mth,v.trx_month)) = 1 then 'HSSUBSIDY_LM'
          when abs(months_between(t.comm_mth,v.trx_month)) = 2 then 'HSSUBSIDY_L2M'
          when abs(months_between(t.comm_mth,v.trx_month)) = 3 then 'HSSUBSIDY_L3M'
          when abs(months_between(t.comm_mth,v.trx_month)-t.yoy_offset + 1) = 1 then 'HSSUBSIDY_YOYLM'
          when abs(months_between(t.comm_mth,v.trx_month)-t.yoy_offset + 1) = 2 then 'HSSUBSIDY_YOYL2M'
          when abs(months_between(t.comm_mth,v.trx_month)-t.yoy_offset + 1) = 3 then 'HSSUBSIDY_YOYL3M'
         else 'HSSUBSIDY_NONE'
end;
commit;
-----------------------------Part b logic -----------------------------------

prompt 'Step B_BM_CRC_COMM_003B01_T : [ Net plan revenue LM ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T
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
  from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
left outer join prd_adw.subr_info_hist s
   on t.lm_cust_num = s.cust_num
   and (t.lm_subr_num = s.subr_num)
   and t.lm_cust_num = s.cust_num
   and &comm_mth - 1 between s.start_date and s.end_date
left outer join prd_adw.bill_serv_ref bs
    on s.rate_plan_cd = bs.bill_serv_cd
   and &comm_mth - 1 between bs.eff_start_date and bs.eff_end_date
union all
------YOY plan tariff -----
select
    t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id
    ,'NP_PLAN_TARIFF_YOYLM' as val_type
    ,nvl(bs.bill_rate,0) as val_amt
    ,nvl(sy.rate_plan_cd,' ') as val_str
    ,' ' as json_rmk
  from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t1
left outer join prd_adw.subr_info_hist sy
   on t1.lm_subr_num = sy.subr_num
--and t1.np_yoy_cust_num = sy.cust_num
   and add_months(&comm_mth ,- t1.np_yoy_offset)-1 between sy.start_date and sy.end_date
   and sy.subr_stat_cd in ('OK','SU','PE')
left outer join prd_adw.cust_info_hist cu
    on sy.cust_num = cu.cust_num    
    and add_months(&comm_mth ,- t1.np_yoy_offset)-1 between cu.start_date and cu.end_date
    and t1.idbr_prefix = cu.hkid_br_prefix   
left outer join prd_adw.bill_serv_ref bs
    on sy.rate_plan_cd = bs.bill_serv_cd
   and add_months(&comm_mth ,- t1.np_yoy_offset)-1 between bs.eff_start_date and bs.eff_end_date
where sy.subr_num is not null and cu.cust_num is not null;
--select
--    t1.trx_mth
--    ,t1.comm_mth
--    ,t1.case_id
--    ,'NP_PLAN_TARIFF_YOYLM' as val_type
--    ,nvl(bs.bill_rate,0) as val_amt
--    ,nvl(sy.rate_plan_cd,' ') as val_str
--    ,' ' as json_rmk
--  from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t1
--left outer join prd_adw.subr_info_hist sy
--   on t1.lm_subr_num = sy.subr_num
--   and t1.np_yoy_cust_num = sy.cust_num
--   and add_months(&comm_mth ,- t1.np_yoy_offset)-1 between sy.start_date and sy.end_date
--   and sy.subr_stat_cd in ('OK','SU','PE')
--left outer join prd_adw.bill_serv_ref bs
--    on sy.rate_plan_cd = bs.bill_serv_cd
--   and add_months(&comm_mth ,- t1.np_yoy_offset)-1 between bs.eff_start_date and bs.eff_end_date;
commit;

prompt 'Step B_BM_CRC_COMM_003B01_T : [ Net plan rebate LM and YOYLM] ';
--LD_RPT_CONT_RENEW_SUM only keep one month data of last month
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T
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
 from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
    ,PRD_BIZ_SUMM_VW.VW_LD_RPT_CONT_RENEW_SUMM_MTH s
 where (t.lm_subr_num = s.subr_num
    or t.split_subr = s.subr_num)   
   and t.lm_cust_num = s.cust_num
   and trunc(s.trx_month,'MM') = add_months(&comm_mth,-1)
  group by t.trx_mth
    ,t.comm_mth
    ,t.case_id
union all
select
     t.trx_mth
    ,t.comm_mth
    ,t.case_id
    ,'NP_PLAN_REBATE_YOYLM' as val_type
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
 from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
    ,PRD_BIZ_SUMM_VW.VW_LD_RPT_CONT_RENEW_SUMM_MTH s
    ,prd_adw.cust_info_hist cu
 where (t.lm_subr_num = s.subr_num
    or t.split_subr = s.subr_num)
    and s.cust_num = cu.cust_num
    and t.idbr_prefix = cu.hkid_br_prefix
    and add_months(&comm_mth ,- t.np_yoy_offset)-1 between cu.start_date and cu.end_date
   --and t.np_yoy_cust_num = s.cust_num
   and trunc(s.trx_month,'MM') = add_months(&comm_mth,-t.np_yoy_offset)
  group by t.trx_mth
    ,t.comm_mth
    ,t.case_id;
--select
--     t.trx_mth
--    ,t.comm_mth
--    ,t.case_id
--    ,'NP_PLAN_REBATE_YOYLM' as val_type
--    ,sum(LM_BONUS_REBATE + LM_SERVICE_FEE_REBATE +
--     case when LM_CALL_GUARD_IN_DOLLAR > 0
--            or ST_PROTECT_BILLCD_TARRIF > 0
--            or ST_PROTECT_ENT_BILLCD_TARIFF > 0
--            or CALL_GRD_ST_PROT_BILLCD_TARIFF > 0
--            or MOBILE_THREAT_PREVENT_BILL_CD <> ' '
--            or TRAVELLER_BILL_CD_TARIFF > 0
--            or DATA_ROAM_PKT_BILL_CD_TARIFF > 0
--     then 0
--     else LM_FEATURE_CREDIT_REBATE
--     end) as val_amt
--     ,' ' as val_str
--     ,' ' as json_rmk
-- from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
--    ,PRD_BIZ_SUMM_VW.VW_LD_RPT_CONT_RENEW_SUMM_MTH s
-- where (t.lm_subr_num = s.subr_num
--    or t.split_subr = s.subr_num)   
--   and t.np_yoy_cust_num = s.cust_num
--   and trunc(s.trx_month,'MM') = add_months(&comm_mth,-t.np_yoy_offset)
--  group by t.trx_mth
--    ,t.comm_mth
--    ,t.case_id;
commit;

prompt 'Step B_BM_CRC_COMM_003B01_T : [ Net plan FUP ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T
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
  from  ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t1
       ,prd_adw.subr_ld_hist sl
       ,(select par_grp,par_id,par_val_str as mkt_cd ,par_val_num as fup_addon_amt
           from ${etlvar::ADWDB}.BM_CRC_COMM_PAR_REF
           where par_grp = 'FUP_ADDON'
             and par_id = 'MKT_CD_LST'
             and &trx_mth between par_eff_s_date and par_eff_e_date
         )p
  where t1.lm_subr_num = sl.subr_num
  and t1.lm_cust_num = sl.cust_num
  and &comm_mth - 1 between sl.start_date and sl.end_date
  and &comm_mth - 1 between sl.ld_start_date and sl.ld_expired_date
  and sl.mkt_cd = p.mkt_cd
  group by  t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id
union all
 Select
     t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id
    ,'NP_PLAN_FUP_YOYLM' as val_type
    ,nvl(max(p.fup_addon_amt),0) as val_amt
    ,nvl(max(sl.inv_num),' ') as val_str
    ,' ' as json_rmk
  from  ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t1
       ,prd_adw.subr_ld_hist sl
       ,(select par_grp,par_id,par_val_str as mkt_cd ,par_val_num as fup_addon_amt
           from ${etlvar::ADWDB}.BM_CRC_COMM_PAR_REF
           where par_grp = 'FUP_ADDON'
             and par_id = 'MKT_CD_LST'
             and &trx_mth between par_eff_s_date and par_eff_e_date
         )p
  where t1.lm_subr_num = sl.subr_num
  and t1.np_yoy_cust_num = sl.cust_num
  and add_months(&comm_mth,-t1.np_yoy_offset)-1 between sl.start_date and sl.end_date  
  and add_months(&comm_mth,-t1.np_yoy_offset)-1  between sl.ld_start_date and sl.ld_expired_date
  and sl.mkt_cd = p.mkt_cd
  group by  t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id ;
commit;

----- Reserver HSF for loading job NP_HSFUND_LM and NP_HSFUND_YOYLM
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T
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
    ,'NP_HSFUND_LM' as val_type
    ,sum(hs.derived_hsf_per_line / hs.hsf_valid_period + hs.hsf_amt_per_mth) as val_amt
     ,' ' as val_str
     ,' ' as json_rmk
 from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
     ,${etlvar::ADWDB}.HS_FUND_FINANCE hs
 where t.ld_inv_num = hs.pos_inv_num
 group by  
     t.trx_mth
    ,t.comm_mth
    ,t.case_id       
union all
 Select
     t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id
    ,'NP_HSFUND_YOYLM' as val_type    
    ,max(hs.derived_hsf_per_line / hs.hsf_valid_period + hs.hsf_amt_per_mth)
            keep(dense_rank first order by sl.ld_expired_date desc) as val_amt        
    ,max(sl.inv_num) keep(dense_rank first order by sl.ld_expired_date desc) as val_str
    ,' ' as json_rmk
  from  ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t1
       ,prd_adw.subr_ld_hist sl
       ,${etlvar::ADWDB}.HS_FUND_FINANCE hs
  where t1.lm_subr_num = sl.subr_num
  and t1.np_yoy_cust_num = sl.cust_num
  and add_months(&comm_mth,-t1.np_yoy_offset)-1 between sl.start_date and sl.end_date
  and add_months(&comm_mth,-t1.np_yoy_offset)-1  between sl.ld_start_date and sl.ld_expired_date
  and sl.inv_num = hs.pos_inv_num
  group by
     t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id ;    
commit;

prompt 'Step B_BM_CRC_COMM_003B01_T : [ Net plan revenue HS subsidy ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T(
        trx_mth
        ,comm_mth
        ,case_id
        ,val_Type
        ,val_amt
        ,val_str
        ,json_rmk) 
select
     t.trx_mth
    ,t.comm_mth
    ,t.case_id
    ,'NP_HS_SUBSIDY_LM' as val_type
    ,sum(hs.amort_amt) as val_amt
     ,' ' as val_str
     ,' ' as json_rmk
 from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
     ,prd_adw.HS_AMORT hs
 where t.ld_inv_num = hs.inv_num  
  and trunc(hs.trx_month,'MM') = add_months(&comm_mth,-1)
 group by
     t.trx_mth
    ,t.comm_mth
    ,t.case_id
 union all
 Select
     t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id
    ,'NP_HS_SUBSIDY_YOYLM' as val_type
    ,sum(hs.amort_amt) as val_amt
    ,' ' as val_str
    ,' ' as json_rmk
  from  ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t1
       ,prd_adw.subr_ld_hist sl
       ,prd_adw.cust_info_hist cu
       ,prd_adw.HS_AMORT hs
  where t1.lm_subr_num = sl.subr_num
  and sl.cust_num = cu.cust_num
  and add_months(&comm_mth,-t1.np_yoy_offset)-1 between cu.start_date and cu.end_date
  and t1.idbr_prefix = cu.hkid_br_prefix
  and add_months(&comm_mth,-t1.np_yoy_offset)-1 between sl.start_date and sl.end_date
  and add_months(&comm_mth,-t1.np_yoy_offset)-1  between sl.ld_start_date and sl.ld_expired_date
  and sl.inv_num = hs.inv_num
  and trunc(hs.trx_month,'MM') = add_months(&comm_mth,-t1.np_yoy_offset)
  group by
     t1.trx_mth
    ,t1.comm_mth
    ,t1.case_id;
-- Select
--     t1.trx_mth
--    ,t1.comm_mth
--    ,t1.case_id
--    ,'NP_HS_SUBSIDY_YOYLM' as val_type
--    ,sum(hs.amort_amt) as val_amt
--    ,' ' as val_str
--    ,' ' as json_rmk
--  from  ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t1
--       ,prd_adw.subr_ld_hist sl
--       ,prd_adw.HS_AMORT hs
--  where t1.lm_subr_num = sl.subr_num
--  and t1.np_yoy_cust_num = sl.cust_num
--  and add_months(&comm_mth,-t1.np_yoy_offset)-1 between sl.start_date and sl.end_date
--  and add_months(&comm_mth,-t1.np_yoy_offset)-1  between sl.ld_start_date and sl.ld_expired_date
--  and sl.inv_num = hs.inv_num
--  and trunc(hs.trx_month,'MM') = add_months(&comm_mth,-t1.np_yoy_offset)
--  group by
--     t1.trx_mth
--    ,t1.comm_mth
--    ,t1.case_id ; 
commit;

--Remark for backup
--insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T
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
--    ,'NP_HS_SUBSIDY_LM' as val_type
--    ,sum(s.amort_amt) as val_amt
--     ,' ' as val_str
--     ,' ' as json_rmk
-- from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
--     ,prd_adw.HS_AMORT s
-- where (t.lm_subr_num = s.subr_num
--    or t.split_subr = s.subr_num)
--  and t.lm_cust_num = s.cust_num
--   and trunc(s.trx_month,'MM') = add_months(&comm_mth,-1)
--  group by t.trx_mth
--    ,t.comm_mth
--    ,t.case_id
-- union all
-- select
--     t.trx_mth
--    ,t.comm_mth
--    ,t.case_id
--    ,'NP_HS_SUBSIDY_YOYLM' as val_type
--    ,sum(s.amort_amt) as val_amt
--    ,' ' as val_str
--    ,' ' as json_rmk
-- from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
--     ,prd_adw.HS_AMORT s     
-- where (t.lm_subr_num = s.subr_num
--    or t.split_subr = s.subr_num)
--   and t.np_yoy_cust_num = s.cust_num
--   and trunc(s.trx_month,'MM') = add_months(&comm_mth,-t.np_yoy_offset)
--  group by t.trx_mth
--    ,t.comm_mth
--    ,t.case_id;
--commit;

prompt 'Step B_BM_CRC_COMM_003B01_T : [ Net plan revenue Company subsidy ] ';
--insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T
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
-- from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
--     ,prd_adw.inv_detail s
-- where (t.lm_subr_num = s.subr_num
--    or t.split_subr = s.subr_num)
--   and s.inv_date between add_months(&comm_mth,-1) and &comm_mth-1   
--   and s.type_name like 'DCOSA%'
--   and s.inv_line_type_cd ='ADJ'
--  group by t.trx_mth
--    ,t.comm_mth
--    ,t.case_id
-- union all
-- select
--     t.trx_mth
--    ,t.comm_mth
--    ,t.case_id
--    ,'NP_COMPANY_SUBSIDY_YOYLM' as val_type
--    ,sum(s.line_amt) as val_amt
--    ,' ' as val_str
--    ,' ' as json_rmk
-- from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
--     ,prd_adw.inv_detail s
-- where (t.lm_subr_num = s.subr_num
--    or t.split_subr = s.subr_num)
--   and t.lm_cust_num = s.cust_num
--   and s.inv_date between trunc(add_months(&comm_mth,-t.np_yoy_offset)-1,'MM') and  add_months(&comm_mth,-t.np_yoy_offset)-1
--   and s.type_name like 'DCOSA%'
--   and s.inv_line_type_cd ='ADJ'
--  group by t.trx_mth
--    ,t.comm_mth
--    ,t.case_id;
--commit;

----------------------------------------------------Preparing figure part finish --------------
prompt 'Step B_BM_CRC_COMM_003C01_T : [ Calculate report ] ';
declare
  cursor cur_bs is  
      Select t.case_id
              ,t.case_type
              ,nvl(sum(decode(tt.val_type,'KPI_LM',tt.val_amt,0)),0)  as KPI_LM
              ,nvl(sum(decode(tt.val_type,'KPI_L2M',tt.val_amt,0)),0) as KPI_L2M
              ,nvl(sum(decode(tt.val_type,'KPI_L3M',tt.val_amt,0)),0) as KPI_L3M
              ,nvl(sum(decode(tt.val_type,'HSFUND_LM',tt.val_amt,0)),0) as HSFUND_LM
              ,nvl(sum(decode(tt.val_type,'HSFUND_L2M',tt.val_amt,0)),0) as HSFUND_L2M
              ,nvl(sum(decode(tt.val_type,'HSFUND_L3M',tt.val_amt,0)),0) as HSFUND_L3M
              ,nvl(sum(decode(tt.val_type,'HSSUBSIDY_LM',tt.val_amt,0)),0) as  HSSUBSIDY_LM
              ,nvl(sum(decode(tt.val_type,'HSSUBSIDY_L2M',tt.val_amt,0)),0) as HSSUBSIDY_L2M
              ,nvl(sum(decode(tt.val_type,'HSSUBSIDY_L3M',tt.val_amt,0)),0) as HSSUBSIDY_L3M
              ,nvl(sum(decode(tt.val_type,'KPI_YOYLM',tt.val_amt,0)),0) as KPI_YOYLM
              ,nvl(sum(decode(tt.val_type,'KPI_YOYL2M',tt.val_amt,0)),0) as KPI_YOYL2M
              ,nvl(sum(decode(tt.val_type,'KPI_YOYL3M',tt.val_amt,0)),0) as KPI_YOYL3M
              ,nvl(sum(decode(tt.val_type,'HSFUND_YOYLM',tt.val_amt,0)),0) as HSFUND_YOYLM
              ,nvl(sum(decode(tt.val_type,'HSFUND_YOYL2M',tt.val_amt,0)),0) as HSFUND_YOYL2M
              ,nvl(sum(decode(tt.val_type,'HSFUND_YOYL3M',tt.val_amt,0)),0) as HSFUND_YOYL3M
              ,nvl(sum(decode(tt.val_type,'HSSUBSIDY_YOYLM',tt.val_amt,0)),0) as HSSUBSIDY_YOYLM
              ,nvl(sum(decode(tt.val_type,'HSSUBSIDY_YOYL2M',tt.val_amt,0)),0) as HSSUBSIDY_YOYL2M
              ,nvl(sum(decode(tt.val_type,'HSSUBSIDY_YOYL3M',tt.val_amt,0)),0) as HSSUBSIDY_YOYL3M
              ,nvl(max(decode(tt.val_type,'KPI_LM',1,0)),0)  as CNT_KPI_LM
              ,nvl(max(decode(tt.val_type,'KPI_L2M',1,0)),0) as CNT_KPI_L2M
              ,nvl(max(decode(tt.val_type,'KPI_L3M',1,0)),0) as CNT_KPI_L3M
              ,nvl(max(decode(tt.val_type,'KPI_YOYLM',1,0)),0)  as CNT_KPI_YOYLM
              ,nvl(max(decode(tt.val_type,'KPI_YOYL2M',1,0)),0) as CNT_KPI_YOYL2M
              ,nvl(max(decode(tt.val_type,'KPI_YOYL3M',1,0)),0) as CNT_KPI_YOYL3M 
          ----- Net plan value NP_HSFUND_LM Not ready----
              ,nvl(sum(decode(tt.val_type,'NP_PLAN_TARIFF_LM',tt.val_amt,0)),0)  as NP_PLAN_TARIFF_LM
              ,nvl(sum(decode(tt.val_type,'NP_HSFUND_LM',tt.val_amt,0)),0)  as NP_HSFUND_LM
              ,nvl(sum(decode(tt.val_type,'NP_PLAN_REBATE_LM',tt.val_amt,0)),0)  as NP_PLAN_REBATE_LM
              ,nvl(sum(decode(tt.val_type,'NP_PLAN_FUP_LM',tt.val_amt,0)),0)  as NP_PLAN_FUP_LM
              ,nvl(sum(decode(tt.val_type,'NP_HS_SUBSIDY_LM',tt.val_amt,0)),0)  as NP_HS_SUBSIDY_LM
              ,nvl(sum(decode(tt.val_type,'NP_COMPANY_SUBSIDY_LM',tt.val_amt,0)),0)  as NP_COMPANY_SUBSIDY_LM
              ,nvl(sum(decode(tt.val_type,'NP_PLAN_TARIFF_YOYLM',tt.val_amt,0)),0)  as NP_PLAN_TARIFF_YOYLM
              ,nvl(sum(decode(tt.val_type,'NP_HSFUND_YOYLM',tt.val_amt,0)),0)  as NP_HSFUND_YOYLM
              ,nvl(sum(decode(tt.val_type,'NP_PLAN_REBATE_YOYLM',tt.val_amt,0)),0)  as NP_PLAN_REBATE_YOYLM
              ,nvl(sum(decode(tt.val_type,'NP_PLAN_FUP_YOYLM',tt.val_amt,0)),0)  as NP_PLAN_FUP_YOYLM
              ,nvl(sum(decode(tt.val_type,'NP_HS_SUBSIDY_YOYLM',tt.val_amt,0)),0)  as NP_HS_SUBSIDY_YOYLM
              ,nvl(sum(decode(tt.val_type,'NP_COMPANY_SUBSIDY_YOYLM',tt.val_amt,0)),0)  as NP_COMPANY_SUBSIDY_YOYLM                            
          from  ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t 
          left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_003B01_T tt           
            on t.case_id = tt.case_id       
        group by t.case_id,t.case_type;
  rs_bs cur_bs%rowtype;
  v_bt_rev_avg3mth number(18,5);
  v_bt_rev_avg3mth_yoy number(18,5);  
  v_cmp_bt_rev_rate number(18,5);  
  v_np_lm_amt number(18,5);
  v_np_yoylm_amt number(18,5);
  v_cmp_bt_np_rate number(18,5);
  v_comm_rate_flg varchar2(50);
  v_json_rmk varchar2(4000);
begin
    open cur_bs;
    loop
        v_bt_rev_avg3mth :=0;
        v_bt_rev_avg3mth_yoy :=0;  
        v_cmp_bt_rev_rate :=0;  
        v_np_lm_amt :=0;
        v_np_yoylm_amt :=0;
        v_cmp_bt_np_rate :=0;
        v_comm_rate_flg := ' ';
        fetch cur_bs into rs_bs;
        exit when cur_bs%NOTFOUND;        
        v_bt_rev_avg3mth := case when (rs_bs.CNT_KPI_LM + rs_bs.CNT_KPI_L2M + rs_bs.CNT_KPI_L3M)=0 
                then 0 
                else 
                    (rs_bs.KPI_LM + rs_bs.KPI_L2M + rs_bs.KPI_L3M
                        -rs_bs.HSFUND_LM - rs_bs.HSFUND_L2M - rs_bs.HSFUND_l3M
                        -rs_bs.HSSUBSIDY_LM - rs_bs.HSSUBSIDY_L2M - rs_bs.HSSUBSIDY_L3M)
                    /( rs_bs.CNT_KPI_LM + rs_bs.CNT_KPI_L2M + rs_bs.CNT_KPI_L3M)
                end;
        v_bt_rev_avg3mth_yoy := case when (rs_bs.CNT_KPI_YOYLM + rs_bs.CNT_KPI_YOYL2M + rs_bs.CNT_KPI_YOYL3M)=0 
                then 0 
                else 
                    (rs_bs.KPI_YOYLM + rs_bs.KPI_YOYL2M + rs_bs.KPI_YOYL3M
                        -rs_bs.HSFUND_YOYLM - rs_bs.HSFUND_YOYL2M - rs_bs.HSFUND_YOYL3M
                        -rs_bs.HSSUBSIDY_YOYLM - rs_bs.HSSUBSIDY_YOYL2M - rs_bs.HSSUBSIDY_YOYL3M)
                    /( rs_bs.CNT_KPI_YOYLM + rs_bs.CNT_KPI_YOYL2M + rs_bs.CNT_KPI_YOYL3M)
                end;
        v_cmp_bt_rev_rate := trunc(case when v_bt_rev_avg3mth_yoy =0 then 0 else (v_bt_rev_avg3mth - v_bt_rev_avg3mth_yoy)/abs(v_bt_rev_avg3mth_yoy) * 100 end );
        ---Calculate net plan value for all commission, np_plan_rebate_lm is  negative 
        v_np_lm_amt :=  rs_bs.NP_PLAN_TARIFF_LM + rs_bs.NP_PLAN_REBATE_LM + rs_bs.NP_PLAN_FUP_LM - rs_bs.NP_HSFUND_LM - rs_bs.NP_HS_SUBSIDY_LM + rs_bs.NP_COMPANY_SUBSIDY_LM;
        v_np_yoylm_amt :=  rs_bs.NP_PLAN_TARIFF_YOYLM + rs_bs.NP_PLAN_REBATE_YOYLM + rs_bs.NP_PLAN_FUP_YOYLM - rs_bs.NP_HSFUND_YOYLM - rs_bs.NP_HS_SUBSIDY_YOYLM + rs_bs.NP_COMPANY_SUBSIDY_YOYLM;
        if rs_bs.case_type ='CASE_BT_5G' then        
            ----Drop > 10%
            ----checking <=-10 then 0.3%
            if v_cmp_bt_rev_rate <= -10 then
                v_comm_rate_flg := 'BT_5G_DROP10';
            else
                    ---- Grow/Drop <10%
                v_comm_rate_flg := case when v_np_lm_amt >= 378 then 'BT_5G_GROW10_TIER2' else 'BT_5G_GROW10_TIER1' end;  
            end if;            
        end if;
        if rs_bs.case_type ='CASE_BT_NON5G' then
              if v_cmp_bt_rev_rate <= -10 then
                v_comm_rate_flg := 'BT_NON5G_DROP10';
              else
                    ----- np_plan_rebate is negative
       --         v_np_yoylm_amt :=  rs_bs.NP_PLAN_TARIFF_YOYLM + rs_bs.NP_PLAN_REBATE_YOYLM + rs_bs.NP_PLAN_FUP_YOYLM - rs_bs.NP_HSFUND_YOYLM - rs_bs.NP_HS_SUBSIDY_YOYLM + rs_bs.NP_COMPANY_SUBSIDY_YOYLM;
                v_comm_rate_flg := case when v_np_lm_amt - v_np_yoylm_amt < 0 then 'BT_NON5G_TIER1'
                                  when v_np_lm_amt - v_np_yoylm_amt = 0 then 'BT_NON5G_TIER2'
                                  when v_np_lm_amt - v_np_yoylm_amt between 1 and 15 then 'BT_NON5G_TIER3'
                                  when v_np_lm_amt - v_np_yoylm_amt > 15 then 'BT_NON5G_TIER4'
                             end; 
              end if;
        end if;
        v_json_rmk :=',"BT_VAL_TYPE":"'
                ||';KPI_LM-L3M='||rs_bs.KPI_LM||'-'||rs_bs.KPI_L2M||'-'||rs_bs.KPI_L3M
                ||';HSFUND_LM-L3M='||rs_bs.HSFUND_LM||'-'||rs_bs.HSFUND_L2M||'-'||rs_bs.HSFUND_L3M
                ||';HSSUBSIDY_LM-L3M='||rs_bs.HSSUBSIDY_LM||'-'||rs_bs.HSSUBSIDY_L2M||'-'||rs_bs.HSSUBSIDY_L3M
                ||';KPI_YOYLM-L3M='||rs_bs.KPI_YOYLM||'-'||rs_bs.KPI_YOYL2M||'-'||rs_bs.KPI_YOYL3M
                ||';HSFUND_YOYLM-L3M='||rs_bs.HSFUND_YOYLM||'-'||rs_bs.HSFUND_YOYL2M||'-'||rs_bs.HSFUND_YOYL2M
                ||';HSSUBSIDY_YOYLM-L3M='||rs_bs.HSSUBSIDY_YOYLM||'-'||rs_bs.HSSUBSIDY_YOYL2M||'-'||rs_bs.HSSUBSIDY_YOYL3M
                ||';CNT_KPI_LM-L3M='||rs_bs.CNT_KPI_LM||'-'||rs_bs.CNT_KPI_L2M||'-'||rs_bs.CNT_KPI_L3M
                ||';CNT_KPI_LM-YOYL3M='||rs_bs.CNT_KPI_YOYLM||'-'||rs_bs.CNT_KPI_YOYL2M||'-'||rs_bs.CNT_KPI_YOYL3M
                ||';NP_PLAN_TARIFF_LM-YOYLM='||rs_bs.NP_PLAN_TARIFF_LM||'-'||rs_bs.NP_PLAN_TARIFF_YOYLM
                ||';NP_HSFUND_LM-YOYLM='||rs_bs.NP_HSFUND_LM||'-'||rs_bs.NP_HSFUND_YOYLM
                ||';NP_PLAN_REBATE_LM-YOYLM='||rs_bs.NP_PLAN_REBATE_LM||'-'||rs_bs.NP_PLAN_REBATE_YOYLM
                ||';NP_PLAN_FUP_LM-YOYLM='||rs_bs.NP_PLAN_FUP_LM||'-'||rs_bs.NP_PLAN_FUP_YOYLM
                ||';NP_HS_SUBSIDY_LM-YOYLM='||rs_bs.NP_HS_SUBSIDY_LM||'-'||rs_bs.NP_HS_SUBSIDY_YOYLM
                ||';NP_COMPANY_SUBSIDY_LM-YOYLM='||rs_bs.NP_COMPANY_SUBSIDY_LM||'-'||rs_bs.NP_COMPANY_SUBSIDY_YOYLM
                ||';CMP_BT_REV_RATE='||v_cmp_bt_rev_rate
                ||'"';
        insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003C01_T
        (    case_id
            ,case_type
            ,np_plan_tariff_lm
            ,np_plan_rebate_lm
            ,np_plan_fup_lm
            ,np_hs_subsidy_lm
            ,np_company_subsidy_lm
            ,np_hsfund_lm
            ,np_plan_tariff_yoylm
            ,np_plan_rebate_yoylm
            ,np_plan_fup_yoylm
            ,np_hs_subsidy_yoylm
            ,np_company_subsidy_yoylm
            ,np_hsfund_yoylm
            ,bt_rev_avg3mth
            ,bt_rev_avg3mth_yoy
            ,cmp_bt_rev_rate
            ,np_lm_amt
            ,np_yoylm_amt
            ,cmp_bt_np_rate
            ,comm_rate_flg
            ,json_rmk
         )values(
             rs_bs.case_id
            ,rs_bs.case_type
            ,rs_bs.np_plan_tariff_lm
            ,rs_bs.np_plan_rebate_lm
            ,rs_bs.np_plan_fup_lm
            ,rs_bs.np_hs_subsidy_lm
            ,rs_bs.np_company_subsidy_lm
            ,rs_bs.np_hsfund_lm
            ,rs_bs.np_plan_tariff_yoylm
            ,rs_bs.np_plan_rebate_yoylm
            ,rs_bs.np_plan_fup_yoylm
            ,rs_bs.np_hs_subsidy_yoylm
            ,rs_bs.np_company_subsidy_yoylm
            ,rs_bs.np_hsfund_yoylm
            ,v_bt_rev_avg3mth
            ,v_bt_rev_avg3mth_yoy
            ,v_cmp_bt_rev_rate
            ,v_np_lm_amt
            ,v_np_yoylm_amt
            ,v_cmp_bt_np_rate
            ,v_comm_rate_flg
            ,v_json_rmk
         );
    end loop;    
    close cur_bs;
    commit;  
end;
/
commit; 

prompt 'Step B_BM_CRC_COMM_003D01_T : [ Preparing the pro rata cases ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003D01_T
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
  from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T t
      ,${etlvar::TMPDB}.B_BM_CRC_COMM_001_T t1
----reserver for BM_CNC_COMM_H ------
      ,(select comm_mth,lm_subr_num,lm_cust_num,case_id,ld_start_date,ld_orig_exp_date,np_lm_amt,contract_mth,np_lm_rev_amt from ${etlvar::ADWDB}.BM_CRC_COMM_H where comm_mth < &comm_mth) rh
where t.case_id = t1.case_id
  and t1.lm_subr_num = rh.lm_subr_num
  and t1.lm_cust_num = rh.lm_cust_num
  and rh.comm_mth < t1.comm_mth
  and rh.ld_orig_exp_date >= t1.ld_start_date
  group by t.case_id;
commit;

prompt 'Step B_BM_CRC_COMM_003_T : [ Calculate commission and to target table ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_003_T
(        case_id
        ,case_type
        ,ld_cd
        ,contract_mth
        ,np_plan_tariff_lm
        ,np_plan_rebate_lm
        ,np_plan_fup_lm
        ,np_hs_subsidy_lm
        ,np_company_subsidy_lm
        ,np_hsfund_lm
        ,np_plan_tariff_yoylm
        ,np_plan_rebate_yoylm
        ,np_plan_fup_yoylm
        ,np_hs_subsidy_yoylm
        ,np_company_subsidy_yoylm
        ,np_hsfund_yoylm
        ,bt_rev_avg3mth
        ,bt_rev_avg3mth_yoy
        ,cmp_bt_rev_rate
        ,np_lm_amt
        ,np_yoylm_amt
        ,np_lm_rev_amt
        ,cmp_bt_np_rate
        ,comm_rate_flg
        ,comm_rate
        ,ttl_comm_amt
        ,prv_case_id
        ,prv_case_src
        ,prv_ld_start_date
        ,prv_ld_orig_exp_date
        ,prv_np_lm_amt
        ,prv_contract_mth
        ,prv_np_lm_rev_amt
        ,skip_flg
        ,except_flg
        ,json_rmk
)select 
         t.case_id
        ,t.case_type
        ,tt.ld_cd
        ,to_number(substr(tt.ld_cd,4,2)) as contract_mth
        ,t.np_plan_tariff_lm
        ,t.np_plan_rebate_lm
        ,t.np_plan_fup_lm
        ,t.np_hs_subsidy_lm
        ,t.np_company_subsidy_lm
        ,t.np_hsfund_lm
        ,t.np_plan_tariff_yoylm
        ,t.np_plan_rebate_yoylm
        ,t.np_plan_fup_yoylm
        ,t.np_hs_subsidy_yoylm
        ,t.np_company_subsidy_yoylm
        ,t.np_hsfund_yoylm
        ,t.bt_rev_avg3mth
        ,t.bt_rev_avg3mth_yoy
        ,t.cmp_bt_rev_rate
        ,t.np_lm_amt
        ,t.np_yoylm_amt
        ----- The real value for caluclating the commission ----
        ,greatest(t.np_lm_amt * to_number(substr(tt.ld_cd,4,2)) 
         - case when nvl(td.prv_case_id,' ') <> ' ' and td.prv_ld_orig_exp_date > t1.ld_start_date 
           then nvl(td.prv_np_lm_rev_amt,0)/td.prv_contract_mth * round(months_between(td.prv_ld_orig_exp_date ,t1.ld_start_date),0)  
           else 0 
         end,0) np_lm_rev_amt
        ,t.cmp_bt_np_rate
        ,t.comm_rate_flg
        ,nvl(p.comm_rate,0)
        ---- np_lm_rev_amt multiple comm_rate ----
        ,greatest(t.np_lm_amt * to_number(substr(tt.ld_cd,4,2)) 
         - case when nvl(td.prv_case_id,' ') <> ' ' and td.prv_ld_orig_exp_date > t1.ld_start_date
           then nvl(td.prv_np_lm_rev_amt,0)/td.prv_contract_mth * round(months_between(td.prv_ld_orig_exp_date ,t1.ld_start_date),0)  
           else 0 
         end,0) * nvl(p.comm_rate,0) as ttl_comm_amt
        ,nvL(td.prv_case_id,' ')
        ,nvL(td.prv_case_src,' ')
        ,nvl(td.prv_ld_start_date,date '2999-12-31')
        ,nvl(td.prv_ld_orig_exp_date,date '2999-12-31')
        ,nvl(td.prv_np_lm_amt,0)
        ,nvl(td.prv_contract_mth,0)
        ,nvl(td.prv_np_lm_rev_amt,0)
        ,case when t.np_lm_amt = 0 or t.np_yoylm_amt =0 then 'Y' else ' ' end skip_flg
        ,case when t.np_lm_amt = 0 or t.np_yoylm_amt =0 then 'EXCEPT_NOFOUND_NP_YOYLM_AMT' else ' ' end except_flg
        ,t.json_rmk||case when nvl(td.prv_case_id,' ') <> ' ' and td.prv_ld_orig_exp_date > t1.ld_start_date then 
                        ',"PRO_RATA_PRV_CASE_INFO":"OVERLAP-'||round(months_between(td.prv_ld_orig_exp_date ,t1.ld_start_date),0)||';PRV_SRC-'||td.prv_case_src||'"'
                     else ' 'end 
   from ${etlvar::TMPDB}.B_BM_CRC_COMM_003A01_T tt
   left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_003C01_T t
        on t.case_id  = tt.case_id
   left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T t1
        on t.case_id = t1.case_id
   left outer join (
        Select 
               par_grp
              ,par_id
              ,par_val_num as comm_rate
          from ${etlvar::ADWDB}.BM_CRC_COMM_PAR_REF
         where par_grp like 'CASE_BT_RATE'
           and &trx_mth between par_eff_s_date and par_eff_e_date
   ) p on t.comm_rate_flg = p.par_id
   left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_003D01_T td
        on tt.case_id = td.case_id ;
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

