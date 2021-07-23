#####################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29
#
#
######################################################


my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
#my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_BM_ICT_COMM_RPT/bin/master_dev.pl";
#require $ETLVAR;

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


execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_001A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_001B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_001C01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_001_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_002A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_002B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_002C01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_002D01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_002_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_003A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_003_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_004_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_005_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_ICT_COMM_RPT_006_T');


set define on;
define tx_date = trunc(to_date('$etlvar::TXDATE','YYYY-MM-DD'),'MM');
define comm_mth = add_months(&tx_date,-1);
define rpt_mth = add_months(&comm_mth,-4);
define rpt_s_date = add_months(&comm_mth,-4);
define rpt_e_date = add_months(&comm_mth,-3)-1;

--------------------------------------------------------------------------------------------------------
DELETE FROM ${etlvar::ADWDB}.BM_ICT_COMM_RPT where comm_mth = &comm_mth;
commit;


---base---
prompt '[base from bill_cd]';
insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T  --667
(        RPT_MTH
        ,COMM_MTH
        ,CASE_ID
        ,CUST_NUM
        ,SUBR_NUM
        ,BILL_SERV_CD
        ,BILL_START_DATE
        ,BILL_END_DATE
        ,SALESMAN_CD
        ,DEALER_CD
        ,BILL_RATE
        ,ICT_TYPE1
        ,ICT_TYPE2
        ,ICT_TYPE3
        ,IS_PLAN_CD
        ,SUBR_SW_ON_DATE
        ,SUBR_SW_OFF_DATE
        ,RATE_PLAN_CD
        ,SUBR_STAT_CD
        ,VENDOR
        ,CREATE_TS
        ,REFRESH_TS
        ,CASE_START_DATE
        ,CASE_SRC
        ,ld_inv_num
        ,PROFILE_MAP_DATE
        ,hkid_br_prefix
        ,bill_salesman_cd
        )
select
       &rpt_mth as rpt_mth,
       &comm_mth as comm_mth,
       'C_' || tmpa.SUBR_NUM || '_' || to_char(tmpa.BILL_START_DATE,'yyyymmdd') || '_' || tmpa.BILL_SERV_CD as CASE_ID,
       tmpa.CUST_NUM,
       tmpa.SUBR_NUM,
       tmpa.BILL_SERV_CD,
       max(tmpa.BILL_START_DATE)keep(dense_rank first order by tmpa.BILL_END_DATE desc),
       max(tmpa.BILL_END_DATE)keep(dense_rank first order by tmpa.BILL_END_DATE desc),
       max(tmpa.SALESMAN_CD)keep(dense_rank first order by tmpa.BILL_END_DATE desc),
       max(tmpa.DEALER_CD)keep(dense_rank first order by tmpa.BILL_END_DATE desc),
       max(brf.bill_rate)keep(dense_rank first order by tmpa.BILL_END_DATE desc),
       max(tmpb.type1)keep(dense_rank first order by tmpa.BILL_END_DATE desc) as ict_type1,
       max(tmpb.type2)keep(dense_rank first order by tmpa.BILL_END_DATE desc) as ict_type2,
       max(tmpb.type3)keep(dense_rank first order by tmpa.BILL_END_DATE desc) as ict_type3,
       max(case when tmpa.bill_serv_cd = p.rate_plan_cd then 'Y' else 'N' end)keep(dense_rank first order by tmpa.BILL_END_DATE desc) is_plan_cd,
       max(nvl(p.subr_sw_on_date,date '2999-12-31'))keep(dense_rank first order by tmpa.BILL_END_DATE desc) as subr_sw_on_date,
       max(nvl(p.subr_sw_off_date,date '2999-12-31'))keep(dense_rank first order by tmpa.BILL_END_DATE desc) as subr_sw_off_date,
       max(nvl(p.rate_plan_cd,' '))keep(dense_rank first order by tmpa.BILL_END_DATE desc) as rate_plan_cd,
       max(nvl(p.subr_stat_cd,' '))keep(dense_rank first order by tmpa.BILL_END_DATE desc) as subr_stat_cd,
       max(nvl(p.corp_attr_24,' '))keep(dense_rank first order by tmpa.BILL_END_DATE desc) as vendor,
       sysdate,
       sysdate,
       max(tmpa.BILL_START_DATE)keep(dense_rank first order by tmpa.BILL_END_DATE desc) as CASE_START_DATE,
       'SRC_BILL_SERVS'    as case_src,
       ' ' as ld_inv_num,
       max(greatest(&rpt_e_date,tmpa.bill_end_date))keep(dense_rank first order by tmpa.BILL_END_DATE desc) profile_map_date,
       max(c.hkid_br_prefix)keep(dense_rank first order by tmpa.BILL_END_DATE desc),
       max(tmpa.SALESMAN_CD)keep(dense_rank first order by tmpa.BILL_END_DATE desc) as bill_salesman_cd
from prd_adw.bill_servs tmpa
left outer join prd_adw.subr_info_hist p
    on tmpa.cust_num = p.cust_num
   and tmpa.subr_num = p.subr_num
   and greatest(&rpt_e_date,tmpa.bill_end_date) between p.start_date and p.end_date
left outer join prd_adw.cust_info_hist c
    on p.cust_num = c.cust_num
   and greatest(&rpt_e_date,tmpa.bill_end_date) between c.start_date and c.end_date
    ,${etlvar::ADWDB}.BM_COMM_RPT_ICT_REF tmpb
    ,prd_adw.bill_serv_ref brf
where
      tmpa.bill_serv_cd = tmpb.bill_cd
   and ((tmpb.TYPE1 in ('Enterprise Solution','Enterprise Solution - Others','Fixed Network Services','ICT Projects','SmartConnect')
        and tmpb.TYPE2 <> 'DRS') or lower(tmpb.type3) = 'Order Recording on Mobile')
   and tmpa.BILL_START_DATE between &rpt_s_date and &rpt_e_date
   and tmpa.bill_serv_cd = brf.bill_serv_cd
   and &rpt_e_date between brf.eff_start_date and brf.eff_end_date
group by
        tmpa.CUST_NUM,
        tmpa.SUBR_NUM,
        tmpa.BILL_SERV_CD ,
        'C_' || tmpa.SUBR_NUM || '_' || to_char(tmpa.BILL_START_DATE,'yyyymmdd') || '_' || tmpa.BILL_SERV_CD;
commit;


prompt '[base from sl ]';

insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T  --667
(        RPT_MTH
        ,COMM_MTH
        ,CASE_ID
        ,CUST_NUM
        ,SUBR_NUM
        ,BILL_SERV_CD
        ,BILL_START_DATE
        ,BILL_END_DATE
        ,SALESMAN_CD
        ,DEALER_CD
        ,BILL_RATE
        ,ICT_TYPE1
        ,ICT_TYPE2
        ,ICT_TYPE3
        ,IS_PLAN_CD
        ,SUBR_SW_ON_DATE
        ,SUBR_SW_OFF_DATE
        ,RATE_PLAN_CD
        ,SUBR_STAT_CD
        ,VENDOR
        ,CREATE_TS
        ,REFRESH_TS
        ,CASE_START_DATE
        ,CASE_SRC
        ,ld_inv_num
        ,PROFILE_MAP_DATE
        ,hkid_br_prefix
        ,bill_salesman_cd
        )
 select
       &rpt_mth as rpt_mth,
       &comm_mth as comm_mth,
       'C_' || bs.SUBR_NUM || '_' || to_char(bs.BILL_START_DATE,'yyyymmdd') || '_' || bs.BILL_SERV_CD as CASE_ID,
       bs.CUST_NUM,
       bs.SUBR_NUM,
       bs.BILL_SERV_CD,
       bs.BILL_START_DATE,
       max(bs.BILL_END_DATE) keep (dense_rank first order by bs.BILL_END_DATE desc) as BILL_END_DATE,
       max(sl.SALESMAN_CD) keep (dense_rank first order by bs.BILL_END_DATE desc) as SALESMAN_CD,
       --max(bs.DEALER_CD) keep (dense_rank first order by bs.BILL_END_DATE desc) as DEALER_CD,
       ' 'as Dealer_cd,
       max(bsr.bill_rate)keep (dense_rank first order by bs.BILL_END_DATE desc) as BILL_END_DATE,
       max(tmpb.type1)keep (dense_rank first order by bs.BILL_END_DATE desc) as ict_type1,
       max(tmpb.type2)keep (dense_rank first order by bs.BILL_END_DATE desc) as ict_type2,
       max(tmpb.type3)keep (dense_rank first order by bs.BILL_END_DATE desc) as ict_type3,
       max(case when bs.bill_serv_cd = cp.rate_plan_cd then 'Y' else 'N' end)keep (dense_rank first order by bs.BILL_END_DATE desc) is_plan_cd,
       max(cp.subr_sw_on_date)keep (dense_rank first order by bs.BILL_END_DATE desc),
       max(cp.subr_sw_off_date)keep (dense_rank first order by bs.BILL_END_DATE desc),
       max(cp.rate_plan_cd)keep (dense_rank first order by bs.BILL_END_DATE desc),
       max(cp.subr_stat_cd)keep (dense_rank first order by bs.BILL_END_DATE desc),
       max(cp.corp_attr_24)keep (dense_rank first order by bs.BILL_END_DATE desc) as vendor,
       sysdate,
       sysdate,
       sl.ld_start_date as case_start_date,
       'SRC_RENEW_LD' as case_src,
       sl.inv_num as ld_inv_num,
       sl.ld_start_date as profile_map_date,
       max(cu.hkid_br_prefix)keep (dense_rank first order by bs.BILL_END_DATE desc) as hkid_br_prefix,
      ' ' as bill_salseman_cd
 from  prd_adw.subr_ld_hist sl
 left outer join prd_adw.subr_info_hist cp
    on sl.subr_num = cp.subr_num
    and sl.cust_num = cp.cust_num
    and &rpt_e_date between cp.start_date and cp.end_date
 left outer join prd_adw.cust_info_hist cu
    on sl.cust_num = cu.cust_num
   and &rpt_e_date between cu.start_date and cu.end_date
      ,prd_adw.bill_servs bs
      ,${etlvar::ADWDB}.BM_COMM_RPT_ICT_REF tmpb
      ,prd_adw.bill_serv_ref bsr
 where sl.ld_start_date between &rpt_s_date and &rpt_e_date
   and sl.mkt_cd = 'RENEW'
   and sl.ld_expired_date >= &rpt_e_date
   and &rpt_e_date between sl.start_date and sl.end_date
   and sl.void_flg <> 'y' and sl.waived_flg <> 'Y' and sl.billed_flg<>'Y'
   and sl.cust_num = bs.cust_num
   and sl.subr_num = bs.subr_num
   and bs.bill_start_date < &rpt_s_date
   and bs.bill_end_date >= sl.ld_start_date
   and bs.bill_serv_cd = tmpb.bill_cd
       and ((tmpb.TYPE1 in ('Enterprise Solution','Enterprise Solution - Others','Fixed Network Services','ICT Projects','SmartConnect')
            and tmpb.TYPE2 <> 'DRS')
       or lower(tmpb.type3) = 'Order Recording on Mobile')
   and bs.bill_serv_cd = bsr.bill_serv_cd
   and &rpt_e_date between bsr.eff_start_date and bsr.eff_end_date
group by   bs.CUST_NUM,
       bs.SUBR_NUM,
       bs.BILL_SERV_CD,
       bs.BILL_START_DATE,
       sl.ld_start_date,
       sl.inv_num  ;
commit;

insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T
(        RPT_MTH
        ,COMM_MTH
        ,CASE_ID
        ,CUST_NUM
        ,SUBR_NUM
        ,BILL_SERV_CD
        ,BILL_START_DATE
        ,BILL_END_DATE
        ,SALESMAN_CD
        ,DEALER_CD
        ,BILL_RATE
        ,ICT_TYPE1
        ,ICT_TYPE2
        ,ICT_TYPE3
        ,IS_PLAN_CD
        ,SUBR_SW_ON_DATE
        ,SUBR_SW_OFF_DATE
        ,RATE_PLAN_CD
        ,SUBR_STAT_CD
        ,VENDOR
        ,CREATE_TS
        ,REFRESH_TS
        ,CASE_START_DATE
        ,CASE_SRC
        ,ld_inv_num
        ,PROFILE_MAP_DATE
        ,hkid_br_prefix
        ,bill_salesman_cd
        )
 select distinct
       &rpt_mth as rpt_mth,  --yyy
       &comm_mth as comm_mth,  --yyy
       'C_' || bs.SUBR_NUM || '_' || to_char(bs.BILL_START_DATE,'yyyymmdd') || '_' || bs.BILL_SERV_CD as CASE_ID,  --yyy
       bs.CUST_NUM,   --yyy  sl.cust_num    --yyy
       bs.SUBR_NUM,   --yyy   sl.subr_num   --yyy
       bs.BILL_SERV_CD,     --yyy
       bs.BILL_START_DATE,   --yyy
       max(bs.BILL_END_DATE) keep (dense_rank first order by bs.BILL_END_DATE desc) as BILL_END_DATE,  --yyy
       max(sl.SALESMAN_CD) keep (dense_rank first order by bs.BILL_END_DATE desc) as SALESMAN_CD,   --yyy
       --max(bs.DEALER_CD) keep (dense_rank first order by bs.BILL_END_DATE desc) as DEALER_CD,
       ' 'as Dealer_cd,   --yyy
       max(bsr.bill_rate)keep (dense_rank first order by bs.BILL_END_DATE desc) as BILL_RATE,   --yyy
       max(tmpb.type1)keep (dense_rank first order by bs.BILL_END_DATE desc) as ict_type1,   --yyy
       max(tmpb.type2)keep (dense_rank first order by bs.BILL_END_DATE desc) as ict_type2,   --yyy
       max(tmpb.type3)keep (dense_rank first order by bs.BILL_END_DATE desc) as ict_type3,   --yyy
       max(case when bs.bill_serv_cd = cp.rate_plan_cd then 'Y' else 'N' end)keep (dense_rank first order by bs.BILL_END_DATE desc) is_plan_cd,  --yyy
       max(cp.subr_sw_on_date)keep (dense_rank first order by bs.BILL_END_DATE desc) as SUBR_SW_ON_DATE,   --yyy
       max(cp.subr_sw_off_date)keep (dense_rank first order by bs.BILL_END_DATE desc) as SUBR_SW_OFF_DATE,   --yyy
       max(cp.rate_plan_cd)keep (dense_rank first order by bs.BILL_END_DATE desc) as RATE_PLAN_CD,  --yyy
       max(cp.subr_stat_cd)keep (dense_rank first order by bs.BILL_END_DATE desc) as SUBR_STAT_CD,  --yyy
       max(cp.corp_attr_24)keep (dense_rank first order by bs.BILL_END_DATE desc) as vendor,   --yyy
       sysdate as CREATE_TS,   --yyy
       sysdate as REFRESH_TS,       --yyy
       max(sl.ld_start_date)keep (dense_rank first order by sl.ld_expired_date desc) as case_start_date,  --yyy
       'SRC_RENEW_SPLITPOS' as case_src,    --yyy
       max(sl.inv_num)keep (dense_rank first order by sl.ld_expired_date desc) as ld_inv_num,  --yyy
       max(sl.ld_start_date)keep (dense_rank first order by sl.ld_expired_date desc) as profile_map_date,  --yyy
       max(cu.hkid_br_prefix)keep (dense_rank first order by bs.BILL_END_DATE desc) as hkid_br_prefix,         --yyy
      ' ' as bill_salseman_cd   --yyy
 from  prd_adw.pos_inv_header p2,
       prd_adw.pos_inv_header p,
       prd_adw.subr_ld_hist sl
  left outer join prd_adw.subr_info_hist cp
    on sl.subr_num = cp.subr_num
    and sl.cust_num = cp.cust_num
    and &rpt_e_date between cp.start_date and cp.end_date
  left outer join prd_adw.cust_info_hist cu
    on sl.cust_num = cu.cust_num
   and &rpt_e_date between cu.start_date and cu.end_date
      ,prd_adw.bill_servs bs
      ,${etlvar::ADWDB}.BM_COMM_RPT_ICT_REF tmpb
      ,prd_adw.bill_serv_ref bsr
where &rpt_e_date between sl.start_date and sl.end_date
  and sl.ld_start_date between &rpt_s_date and &rpt_e_date
  and sl.ld_expired_date >= &rpt_e_date
  and sl.inv_num = p2.inv_num
  and sl.void_flg <> 'Y'
  and sl.waived_flg <> 'Y'
  and sl.billed_flg <> 'Y'
  and p.mkt_cd = 'RENEW'
  and p.ld_cd = ' '
  and p.case_id <> ' '
---- for performance---
  and p.inv_date >= add_months(&rpt_s_date ,-24)
----and p.case_id = p2.case_id
  and months_between (trunc(p.inv_date,'mm'),trunc(p2.inv_date,'mm')) between -1 and 1
  and p.cust_num = p2.cust_num
  and p.subr_num = p2.subr_num
---- for performance---
  and p2.inv_date >= add_months(&rpt_s_date ,-24)
  and p2.ld_cd <> ' '
  and p2.case_id <> ' '
  and p2.inv_num not in(
    select inv_num
      from prd_adw.pos_return_header
     where trx_date between add_months(&rpt_s_date,-24) and &comm_mth - 1)
  and p2.inv_num not in (
        select ld_inv_num from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T
  )
    and sl.cust_num = bs.cust_num
   and sl.subr_num = bs.subr_num
   and bs.bill_start_date < &rpt_s_date
   and bs.bill_end_date >= sl.ld_start_date
   and bs.bill_serv_cd = tmpb.bill_cd
   and bs.bill_serv_cd = bsr.bill_serv_cd
       and ((tmpb.TYPE1 in ('Enterprise Solution','Enterprise Solution - Others','Fixed Network Services','ICT Projects','SmartConnect')
            and tmpb.TYPE2 <> 'DRS')
       or lower(tmpb.type3) = 'Order Recording on Mobile')
   and &rpt_e_date between bsr.eff_start_date and bsr.eff_end_date
group by bs.CUST_NUM,
       bs.SUBR_NUM,
       bs.BILL_SERV_CD,
       bs.BILL_START_DATE;
       --sl.ld_start_date,
       --sl.inv_num;

commit;
--------------------------------------------------------------------------------------------------------
-----##### Start to map LD ###-----
------ map those renew case already with LD in 001A01_T
prompt '[map ld for renew ld case ]';
insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T
(
    rpt_mth
    ,comm_mth
    ,case_id
    ,cust_num
    ,subr_num
    ,is_plan_cd
    ,ld_inv_num
    ,ld_start_date
    ,ld_expired_date
    ,ld_mkt_cd
    ,ld_cd
    ,create_ts
    ,refresh_ts
)
select
     tmpa.rpt_mth
    ,tmpa.comm_mth
    ,tmpa.case_id
    ,tmpa.cust_num
    ,tmpa.subr_num
    ,tmpa.is_plan_cd
    ,tmpb.inv_num as ld_inv_num
    ,tmpb.ld_start_date
    ,tmpb.ld_expired_date
    ,tmpb.mkt_cd ld_mkt_cd
    ,tmpb.ld_cd
    ,sysdate create_ts
    ,sysdate refresh_ts
  from  ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T tmpa
       ,prd_adw.subr_ld_hist tmpb
 where
       tmpa.ld_inv_num = tmpb.inv_num
   and &rpt_e_date between  tmpb.start_date and tmpb.end_date
   and tmpa.ld_inv_num <> ' '
   and tmpa.case_src ='SRC_RENEW_LD'
   and tmpa.case_id not in (select tt.case_id from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T tt);

commit;


--------------------------------------------------------------------------------------------------------
--- Order Recording on Mobile
--insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T
--(
--    rpt_mth
--    ,comm_mth
--    ,case_id
--    ,cust_num
--    ,subr_num
--    ,is_plan_cd
--    ,ld_inv_num
--    ,ld_start_date
--    ,ld_expired_date
--    ,ld_mkt_cd
--    ,ld_cd
--    ,create_ts
--    ,refresh_ts
--)
--select
--     tmpa.rpt_mth
--    ,tmpa.comm_mth
--    ,tmpa.case_id
--    ,tmpa.cust_num
--    ,tmpa.subr_num
--    ,tmpa.is_plan_cd
--    ,tmpb.inv_num as ld_inv_num
--    ,tmpb.ld_start_date
--    ,tmpb.ld_expired_date
--    ,tmpb.mkt_cd as ld_mkt_cd
--    ,tmpb.ld_cd
--    ,sysdate create_ts
--    ,sysdate refresh_ts
--  from  ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T tmpa
--       ,prd_adw.subr_ld_hist tmpb
-- where  tmpa.cust_num = tmpb.cust_num
--   and tmpa.subr_num = tmpb.subr_num
--   and tmpa.case_start_date between tmpb.start_date and tmpb.end_date
--   and tmpa.case_start_date between tmpb.ld_start_date and tmpb.LD_EXPIRED_DATE
--   and tmpa.ICT_TYPE3 = 'Order Recording on Mobile'
--   -- mod120415 Remark for Order Recording on Mobile may not be a plan ld ---
--   --and tmpb.mkt_cd in (select mkt_cd from prd_adw.mkt_ref_vw where ld_revenue ='P')
--   and tmpb.void_flg<>'Y' and tmpb.waived_flg<>'Y' and tmpb.BILLED_FLG<>'Y'
--   and tmpa.case_id not in (select tt.case_id from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T tt)   --?
--   and trunc(tmpa.comm_mth,'mm') = trunc(tmpb.ld_start_date,'mm');

--commit;


prompt '[map ld for other case ]';
--------------------------------------------------------------------------------------------------------
---- mapping those is plan but still have no LD
insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T
(
     rpt_mth
    ,comm_mth
    ,case_id
    ,cust_num
    ,subr_num
    ,is_plan_cd
    ,ld_inv_num
    ,ld_start_date
    ,ld_expired_date
    ,ld_mkt_cd
    ,ld_cd
    ,create_ts
    ,refresh_ts
)
select
     tmpa.rpt_mth
    ,tmpa.comm_mth
    ,tmpa.case_id
    ,tmpa.cust_num
    ,tmpa.subr_num
    ,tmpa.is_plan_cd
    ,max(tmpa.inv_num) keep(dense_rank first order by tmpa.tot desc,tmpa.ld_expired_date desc)
    ,max(tmpa.ld_start_date)   keep(dense_rank first order by tmpa.tot desc,tmpa.ld_expired_date desc)
    ,max(tmpa.ld_expired_date)  keep(dense_rank first order by tmpa.tot desc,tmpa.ld_expired_date desc)
    ,max(tmpa.mkt_cd) keep(dense_rank first order by tmpa.tot desc,tmpa.ld_expired_date desc)
    ,max(tmpa.ld_cd)  keep(dense_rank first order by tmpa.tot desc,tmpa.ld_expired_date desc)
    ,sysdate create_ts
    ,sysdate refresh_ts
  from
  (
select
     tmpa.rpt_mth
    ,tmpa.comm_mth
    ,tmpa.case_id
    ,tmpa.cust_num
    ,tmpa.subr_num
    ,tmpa.is_plan_cd
    ,tmpb.inv_num
    ,tmpb.ld_start_date
    ,tmpb.ld_expired_date
    ,tmpb.mkt_cd
    ,tmpb.ld_cd
    ,case when tmpb.ld_cd like 'LDJ%M%'
          then to_number(substr(tmpb.ld_cd,4,2)) * to_number(substr(tmpb.ld_cd,7))
          else to_number(substr(tmpb.ld_cd,7))
     end as tot
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T tmpa
       ,prd_adw.subr_ld_hist tmpb
 where tmpa.cust_num = tmpb.cust_num
   and tmpa.subr_num = tmpb.subr_num
   and &comm_mth - 1 between tmpb.start_date and tmpb.end_date
  -- and &rpt_e_date between tmpb.ld_start_date and tmpb.LD_EXPIRED_DATE
--   and tmpa.is_plan_cd = 'Y'
   and trunc(tmpa.rpt_mth,'mm') = trunc(tmpb.ld_start_date,'mm')   --?
   and (tmpb.ld_cd like 'LDJ%M%' or tmpb.ld_cd like 'LDJ%F%')
   and tmpb.void_flg<>'Y' and tmpb.waived_flg<>'Y' and tmpb.BILLED_FLG<>'Y'
   and tmpa.case_id not in (select tt.case_id from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T tt)
   ) tmpa
group by
     tmpa.rpt_mth
    ,tmpa.comm_mth
    ,tmpa.case_id
    ,tmpa.cust_num
    ,tmpa.subr_num
    ,tmpa.is_plan_cd;

commit;
--------------------------------------------------------------------------------------------------------
------ override those is_plan_cd ='N' but exists a is_plan_cd='Y' with LD in current month
--insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T
--(
--    rpt_mth
--    ,comm_mth
--    ,case_id
--    ,cust_num
--    ,subr_num
--    ,is_plan_cd
--    ,ld_inv_num
--    ,ld_start_date
--    ,ld_expired_date
--    ,ld_mkt_cd
--    ,ld_cd
       end as JSON_RMK,
--    ,create_ts
--    ,refresh_ts
--)
--select
--     tmpa.rpt_mth
--    ,tmpa.comm_mth
--    ,tmpa.case_id
--    ,tmpa.cust_num
--    ,tmpa.subr_num
--    ,tmpa.is_plan_cd
--    ,max(tmpb.ld_inv_num) keep (dense_rank first order by tmpb.ld_expired_date desc)
--    ,max(tmpb.ld_start_date ) keep (dense_rank first order by tmpb.ld_expired_date desc)
--    ,max(tmpb.ld_expired_date  ) keep (dense_rank first order by tmpb.ld_expired_date desc)
--    ,max(tmpb.ld_mkt_cd ) keep (dense_rank first order by tmpb.ld_expired_date desc)
--    ,max(tmpb.ld_cd ) keep (dense_rank first order by tmpb.ld_expired_date desc)
--      ,sysdate
--      ,sysdate
--from  ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T tmpa
--     ,${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T tmpb
--where tmpa.is_plan_cd ='N'
--  and tmpa.cust_num = tmpb.cust_num
--  and tmpa.subr_num = tmpb.subr_num
--  and tmpb.is_plan_cd ='Y'
--  and tmpa.case_id not in (select tt.case_id from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T tt)
--group by   tmpa.rpt_mth
--    ,tmpa.comm_mth
--    ,tmpa.case_id
--    ,tmpa.cust_num
--    ,tmpa.subr_num
--    ,tmpa.is_plan_cd;

commit;
--------------------------------------------------------------------------------------------------------

-----end map ld
prompt '[handling salesman code ]';
insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001C01_T
(    rpt_mth
    ,comm_mth
    ,case_id
    ,bill_salesman_cd
    ,bill_acct_mgr
    ,bill_team_head
    ,ld_salesman_cd
    ,ld_acct_mgr
    ,ld_team_head
    ,nom_acct_mgr
    ,nom_team_head
    ,fin_salesman_cd
    ,fin_acct_mgr
    ,fin_team_head
    ,json_rmk
    ,skip_flg
    ,create_ts
    ,refresh_ts
)  Select
        t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.bill_salesman_cd
        ,nvl(ss.acct_mgr_fullname,' ')
        ,nvl(ss.team_head,' ')
        ,nvl(ph.salesman_cd,' ')as ld_salsman_cd
        ,nvl(sph.acct_mgr_fullname,' ')
        ,nvl(sph.team_head,' ')
        ,nvl(cl.account_mgr,' ') as n_acct_mgr
        ,nvl(cl.team_head,' ') as n_team_head
        ,case when ss.acct_mgr_fullname is not null then t.bill_salesman_cd
              when sph.acct_mgr_fullname is not null then ph.salesman_cd
              when cl.account_mgr is not null then cl.account_mgr
              else ' '
         end salesman_cd
        ,case when ss.acct_mgr_fullname is not null then ss.acct_mgr_fullname
              when sph.acct_mgr_fullname is not null then sph.acct_mgr_fullname
              when cl.account_mgr is not null then cl.account_mgr
              else ' '
         end fin_acct_mgr
        ,case when ss.team_head is not null then ss.team_head
              when sph.team_head is not null then sph.team_head
              when cl.team_head is not null then cl.team_head
              else ' '
         end fin_team_head
        ,',"SALESINFO_BILL":'||'"'||nvl(t.bill_salesman_cd,'NA')||'-'||nvl(ss.acct_mgr_fullname,'NA')||'-'||nvl(ss.team_head,'NA')||'"'
        ||',"SALESINFO_LD":'||'"'||nvl(ph.salesman_cd,'NA')||'-'||nvl(sph.acct_mgr_fullname,'NA')||'-'||nvl(sph.team_head,'NA')||'"'
        ||',"SALESINFO_NOMINATION":'||'"'||nvl(cl.account_mgr,'NA')||'-'||nvl(cl.account_mgr,'NA')||'-'||nvl(cl.team_head,'NA')||'"' as json_rmk
        ,case when ss.acct_mgr_fullname is null  and sph.acct_mgr_fullname is null and cl.account_mgr is null then 'SKIP_UNMAP_SALESMAN' else ' ' end skip_flg
        ,sysdate
        ,sysdate
 from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T t
 left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T b
   on t.case_id = b.case_id
 left outer join prd_adw.pos_inv_header ph
    on b.ld_inv_num = ph.inv_num
 left outer join  prd_adw.subr_ac_mgr_hist cl
   on t.hkid_br_prefix = cl.idbr_prefix
  and &rpt_e_date between cl.start_date and cl.end_date
 left outer join ${etlvar::ADWDB}.bm_staff_list ss
   on t.salesman_cd = ss.salesman_cd
      and ss.TRX_MTH = &rpt_mth
 left outer join ${etlvar::ADWDB}.bm_staff_list sph
   on ph.salesman_cd = sph.salesman_cd
      and sph.TRX_MTH = &rpt_mth;
commit;

insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T   --667
(
        rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,bill_serv_cd
        ,bill_start_date
        ,bill_end_date
        ,salesman_cd
        ,dealer_cd
        ,bill_rate
        ,ict_type1
        ,ict_type2
        ,ict_type3
        ,is_plan_cd
        ,subr_sw_on_date
        ,subr_sw_off_date
        ,rate_plan_cd
        ,subr_stat_cd
        ,vendor
        ,ld_inv_num
        ,ld_start_date
        ,ld_expired_date
        ,ld_mkt_cd
        ,ld_cd
        ,payment_flg
        ,contract_mth
        ,min_subr_sw_on_date
        ,skip_flg
        ,json_rmk
        ,map_acct_mgr
        ,map_team_head
        ,create_ts
        ,refresh_ts
)
select tmpa.RPT_MTH,
       tmpa.COMM_MTH,
       tmpa.CASE_ID,
       tmpa.CUST_NUM,
       tmpa.SUBR_NUM,
       tmpa.BILL_SERV_CD,
       tmpa.BILL_START_DATE,
       tmpa.BILL_END_DATE,
       tmpa.SALESMAN_CD,
       tmpa.DEALER_CD,
       tmpa.BILL_RATE,
       tmpa.ICT_TYPE1,
       tmpa.ICT_TYPE2,
       tmpa.ICT_TYPE3,
       tmpa.IS_PLAN_CD,
       tmpa.SUBR_SW_ON_DATE,
       tmpa.SUBR_SW_OFF_DATE,
       tmpa.RATE_PLAN_CD,
       tmpa.SUBR_STAT_CD,
       tmpa.VENDOR,
       nvl(sl.ld_inv_num ,' ') as ld_inv_num,
       nvl(sl.ld_start_date,date '2999-12-31'),
       nvl(sl.ld_expired_date,date '1900-01-01'),
       nvl(sl.ld_mkt_cd,' ')ld_mkt_cd,
       nvl(sl.LD_CD,' '),
       case when nvl(sl.ld_inv_num, ' ' ) <> ' ' then 'ICT_CONTRACTED'
            else 'ICT_ONEOFF'
       end as PAYMENT_FLG,
       case when nvl(sl.ld_inv_num, ' ' ) <> ' ' then substr(sl.LD_CD,4,2)
            else '1'
       end as CONTRACT_MTH,
       nvl(mi.min_subr_sw_on_date,tmpa.subr_sw_on_date),
       sales.skip_flg,
       sales.json_rmk as JSON_RMK,
       sales.fin_acct_mgr map_acct_mgr,
       sales.fin_team_head map_team_head,
       sysdate,
       sysdate
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T tmpa
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001B01_T sl
on tmpa.case_id = sl.case_id
left outer join (
    Select t.case_id
           ,min(s.subr_sw_on_date) min_subr_sw_on_date
    from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001A01_T t
        ,prd_adw.subr_info_hist s
        ,prd_adw.cust_info_hist c
    where s.start_date <= &rpt_e_date
      and s.end_date between c.start_date and c.end_date
      and s.cust_num = c.cust_num
      and t.subr_num = s.subr_num
      and t.hkid_br_prefix = c.hkid_br_prefix
      group by t.case_id
)mi
 on tmpa.case_id = mi.case_id
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001C01_T  sales
 on tmpa.case_id = sales.case_id ;

commit;

--------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------

insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002A01_T   --667
(
         rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,bill_serv_cd
        ,vendor
        ,ld_cd
        ,payment_flg
        ,contract_mth
        ,cal_flag
        ,cost_src
        ,json_rmk
        ,create_ts
        ,refresh_ts
)
select RPT_MTH,
       COMM_MTH,
       CASE_ID,
       CUST_NUM,
       SUBR_NUM,
       BILL_SERV_CD,
       VENDOR,
       LD_CD,
       PAYMENT_FLG,
       CONTRACT_MTH,
       case when tmpa.payment_flg='ICT_CONTRACTED' and tmpa.vendor = 'WTT'
            then TMPB.WTT_MTHLY
            when tmpa.payment_flg='ICT_CONTRACTED' and tmpa.vendor = 'HKBN'
            then TMPB.HKBN_MTHLY
            when tmpa.payment_flg='ICT_CONTRACTED' and tmpa.vendor = 'HGC'
            then TMPB.HGC_MTHLY
            when tmpa.payment_flg='ICT_ONEOFF' and tmpa.vendor = 'WTT'
            then TMPB.WTT_ONEOFF
            when tmpa.payment_flg='ICT_ONEOFF' and tmpa.vendor = 'HKBN'
            then TMPB.HKBN_ONEOFF
            when tmpa.payment_flg='ICT_ONEOFF' and tmpa.vendor = 'HGC'
            then TMPB.HGC_ONEOFF
            else ' '
       end as cal_flag,
       case when tmpa.vendor not in ('WTT','HKBN','HGC')
            then ' '
            else 'COST_' || tmpa.vendor || '_' ||decode(tmpa.payment_flg,'ICT_CONTRACTED','MTHLY','ONEOFF')
       end as COST_SRC,
       case when tmpa.vendor not in ('WTT','HKBN','HGC')
            then ' '
            else '"' || 'V6_' || decode(tmpa.payment_flg,'ICT_CONTRACTED','MTHLY_COST','ONEOFF_COST') || '"' || ':' || '"' ||
                case when tmpa.payment_flg='ICT_CONTRACTED' and tmpa.vendor = 'WTT'
                then 'WTT_MTHLY' || ' ' || TMPB.WTT_MTHLY
                when tmpa.payment_flg='ICT_CONTRACTED' and tmpa.vendor = 'HKBN'
                then 'HKBN_MTHLY' || ' ' || TMPB.HKBN_MTHLY
                when tmpa.payment_flg='ICT_CONTRACTED' and tmpa.vendor = 'HGC'
                then 'HGC_MTHLY' || ' ' || TMPB.HGC_MTHLY
                when tmpa.payment_flg='ICT_ONEOFF' and tmpa.vendor = 'WTT'
                then 'WTT_ONEOFF' || ' ' || TMPB.WTT_ONEOFF
                when tmpa.payment_flg='ICT_ONEOFF' and tmpa.vendor = 'HKBN'
                then 'HKBN_ONEOFF' || ' ' || TMPB.HKBN_ONEOFF
                when tmpa.payment_flg='ICT_ONEOFF' and tmpa.vendor = 'HGC'
                then 'HGC_ONEOFF' || ' ' || TMPB.HGC_ONEOFF
                else ' '
                end || '"'
       end as JSON_RMK,
       sysdate,
       sysdate
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T tmpa
left outer join ${etlvar::ADWDB}.BM_COMM_RPT_ICT_REF tmpb
on tmpa.BILL_SERV_CD = TMPB.BILL_CD;

commit;
--------------------------------------------------------------------------------------------------------

--667
declare
    cursor subsidy_ref_cur
       is
       select a.RPT_MTH,a.COMM_MTH,
              a.CASE_ID,a.CUST_NUM,a.SUBR_NUM,
              a.BILL_SERV_CD,a.VENDOR,a.LD_CD,
              a.PAYMENT_FLG,a.CONTRACT_MTH,a.CAL_FLAG,
              a.COST_SRC,a.JSON_RMK
       from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002A01_T a;

    v_cnt number(18);
    v_COST_AMT number;

    function get_cost
    (
       p_PERIOD in number,
       p_VENDOR_str in varchar2
    )return number as
       v_result_cost number;
       v_num number;
       v_idx number;
       v_str varchar2(100);
       v_p_h_str varchar2(100);
       v_p_h_idx number;
       v_p_p_str varchar2(100);
       v_p_p_idx number;
       v_start_p varchar2(100);
       v_end_p varchar2(100);
       v_cost varchar2(100);
    begin
       v_result_cost := 0;
       v_str := p_VENDOR_str || '|';
       v_num := length(v_str) - length(replace(v_str, '|', ''));
       while v_num > 0 loop
           v_idx := instr(v_str,'|');
           v_p_h_str := substr(v_str,1,v_idx - 1);
           v_p_h_idx := instr(v_p_h_str,':');
           v_p_p_str := substr(v_p_h_str,1,v_p_h_idx - 1);
           v_cost := substr(v_p_h_str,v_p_h_idx + 1);
           v_p_p_idx := instr(v_p_p_str,'-');
           v_start_p := substr(v_p_p_str,1,v_p_p_idx - 1);
           v_end_p := substr(v_p_p_str,v_p_p_idx + 1);
           if to_number(v_start_p) <= p_PERIOD and p_PERIOD <= to_number(v_end_p) then
               v_result_cost := to_number(v_cost);
           else
               v_result_cost := 0;
           end if;
           exit when v_result_cost <> 0;
           v_str:= substr(v_str,v_idx + 1);
           v_num := length(v_str) - length(replace(v_str, '|', ''));
       end loop;
    return v_result_cost;
    end;

begin
    v_cnt := 0;

    for subsidy_ref in subsidy_ref_cur
    loop
        v_COST_AMT := get_cost(subsidy_ref.CONTRACT_MTH,subsidy_ref.CAL_FLAG);
        insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002B01_T(RPT_MTH,COMM_MTH,
               CASE_ID,CUST_NUM,SUBR_NUM,
               BILL_SERV_CD,VENDOR,LD_CD,
               PAYMENT_FLG,CONTRACT_MTH,CAL_FLAG,
               COST_SRC,COST_AMT,JSON_RMK,
               CREATE_TS,REFRESH_TS)
        values(subsidy_ref.RPT_MTH,subsidy_ref.COMM_MTH,
               subsidy_ref.CASE_ID,subsidy_ref.CUST_NUM,subsidy_ref.SUBR_NUM,
               subsidy_ref.BILL_SERV_CD,subsidy_ref.VENDOR,subsidy_ref.LD_CD,
               subsidy_ref.PAYMENT_FLG,subsidy_ref.CONTRACT_MTH,subsidy_ref.CAL_FLAG,
               subsidy_ref.COST_SRC,v_COST_AMT,subsidy_ref.JSON_RMK,
               sysdate,sysdate);
        v_cnt := v_cnt+1;
        IF(mod(v_cnt,5000) = 0) THEN
           commit;
        END IF;
    end loop;
    commit;
    exception when others then
        dbms_output.put_line('SQLCODE:' || SQLCODE);
        dbms_output.put_line('SQLERRM:' || SQLERRM);
        raise_application_error(-20001,'Error - '||SQLCODE||' -Messge '||SQLERRM);
    rollback;
end;
/

commit;
--------------------------------------------------------------------------------------------------------
--4   ?
insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002C01_T
(
         case_id
        ,subr_num
        ,ex_monthly_cost
        ,create_ts
        ,refresh_ts
)
select tmpa.case_id,
       tmpa.subr_num,
       nvl(sum(round(tmpb.MONTHLY_COST / (length(tmpb.subr_num) - length(replace(tmpb.subr_num, ',', '')) + 1))),0) as EX_MONTHLY_COST,
       sysdate,
       sysdate
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T tmpa
left outer join mig_adw.BM_COMM_RPT_ICT_EXCOST tmpb
    on to_number(instr(','||replace(tmpb.subr_num,' ','')||',',','||tmpa.subr_num||',')) > 0
where tmpa.ICT_TYPE1 = 'Fixed Network Services'
      and to_number(instr(','||replace(tmpb.subr_num,' ','')||',',','||tmpa.subr_num||',')) > 0
group by tmpa.case_id,tmpa.subr_num;

commit;
--------------------------------------------------------------------------------------------------------


insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002D01_T
(
         rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,is_plan_cd
        ,ex_monthly_cost
        ,payment_flg
        ,contract_mth
        ,cal_flag
        ,cost_src
        ,cost_amt
        ,json_rmk
        ,create_ts
        ,refresh_ts
)
select tmpa.RPT_MTH,
       tmpa.COMM_MTH,
       tmpa.CASE_ID,
       tmpa.CUST_NUM,
       tmpa.SUBR_NUM,
       tmpa.is_plan_cd,
       nvl(tmpb.EX_MONTHLY_COST,0),
       tmpa.PAYMENT_FLG,
       tmpa.CONTRACT_MTH,
       tmpc.CAL_FLAG,
       case when tmpa.is_plan_cd = 'Y' and tmpb.EX_MONTHLY_COST is not null
                 then 'COST_RESELL'
            else tmpc.COST_SRC
       end as COST_SRC,
       case when tmpa.is_plan_cd = 'Y' and tmpb.EX_MONTHLY_COST is not null
                 then tmpb.EX_MONTHLY_COST
            else tmpc.COST_AMT
       end as COST_AMT,
       case when tmpa.is_plan_cd = 'Y' and tmpb.EX_MONTHLY_COST is not null
                 then '"' || 'RESELL_MTHLY_COST' || '"' || ':' || '"' || tmpb.EX_MONTHLY_COST || '"'
            else tmpc.JSON_RMK
       end as JSON_RMK,
       sysdate,
       sysdate
       --tmpb.subr_num,
       --tmpb.EX_MONTHLY_COST,
       --tmpa.is_plan_cd
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T tmpa
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002C01_T tmpb
on tmpa.subr_num = tmpb.subr_num
   and tmpa.case_id = tmpb.case_id
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002B01_T tmpc
on tmpa.case_id = tmpc.case_id;

commit;
--------------------------------------------------------------------------------------------------------

insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002_T
(
         rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,cost_src
        ,cost_amt
        ,total_cost_amt
        ,json_rmk
        ,create_ts
        ,refresh_ts
)
select tmpa.RPT_MTH,
       tmpa.COMM_MTH,
       tmpa.CASE_ID,
       tmpa.CUST_NUM,
       tmpa.SUBR_NUM,
       tmpa.COST_SRC,
       tmpa.COST_AMT,
       case when tmpa.IS_PLAN_CD = 'Y' and tmpa.EX_MONTHLY_COST is not null
            then tmpa.COST_AMT * tmpa.CONTRACT_MTH    --?
            when tmpa.IS_PLAN_CD = 'Y' and tmpa.PAYMENT_FLG = 'ICT_ONEOFF'
            then tmpa.COST_AMT + tmpc.ONEOFF_COST + tmpc.ADD_ONEOFF_COST
            when tmpa.IS_PLAN_CD = 'Y' and tmpa.PAYMENT_FLG = 'ICT_CONTRACTED'
            then (tmpa.COST_AMT + tmpc.MONTHLY_COST) * tmpa.CONTRACT_MTH
            else 0
       end as TOTAL_COST_AMT,
       tmpa.JSON_RMK,
       sysdate,
       sysdate
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002D01_T tmpa
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T tmpb
    on tmpa.case_id = tmpb.case_id
left outer join ${etlvar::ADWDB}.BM_COMM_RPT_ICT_REF tmpc
    on tmpb.BILL_SERV_CD = tmpc.bill_cd;




commit;
--------------------------------------------------------------------------------------------------------


insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_003A01_T  --667
(
         rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,payment_flg
        ,contract_mth
        ,bill_reference
        ,np_tariff
        ,np_rebate
        ,np_total
        ,json_rmk
        ,create_ts
        ,refresh_ts
)
select
    tmpa.RPT_MTH,
    tmpa.COMM_MTH,
    tmpa.CASE_ID,
    tmpa.CUST_NUM,
    tmpa.SUBR_NUM,
    tmpa.Payment_flg,
    tmpa.Contract_mth,
    case when tmpa.Payment_flg = 'ICT_CONTRACTED'
         then ' '
         else tmpc.BILL_REFERENCE
    end as BILL_REFERENCE,
    case when tmpa.Payment_flg = 'ICT_CONTRACTED'
         then tmpa.BILL_RATE
         else tmpc.NP_TARIFF
    end as NP_TARIFF,
    case when  tmpa.is_plan_cd ='Y'
        --tmpa.Payment_flg = 'ICT_CONTRACTED' and tmpa.IS_PLAN_CD = 'Y'
         then tmpb.NP_PLAN_REBATE_LM
         else 0
    end as NP_REBATE,
    0 as NP_TOTAL,
    ' ' as JSON_RMK,
    sysdate,
    sysdate
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T tmpa
left outer join (
 select
    tmpa.RPT_MTH,
    tmpa.COMM_MTH,
    tmpa.CASE_ID,
    nvl(sum(tmpb.LM_BONUS_REBATE + tmpb.LM_SERVICE_FEE_REBATE +
        case when tmpb.LM_CALL_GUARD_IN_DOLLAR > 0
               or tmpb.ST_PROTECT_BILLCD_TARRIF > 0
               or tmpb.ST_PROTECT_ENT_BILLCD_TARIFF > 0
               or tmpb.CALL_GRD_ST_PROT_BILLCD_TARIFF > 0
               or tmpb.MOBILE_THREAT_PREVENT_BILL_CD <> ' '
               or tmpb.TRAVELLER_BILL_CD_TARIFF > 0
               or tmpb.DATA_ROAM_PKT_BILL_CD_TARIFF > 0
        then 0
        else tmpb.LM_FEATURE_CREDIT_REBATE
        end),0) as NP_PLAN_REBATE_LM
 from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T tmpa
 left outer join PRD_BIZ_SUMM_VW.VW_LD_RPT_CONT_RENEW_SUMM_MTH tmpb
 on
        --tmpa.cust_num = tmpb.cust_num
   --and
        tmpa.subr_num = tmpb.subr_num
   and add_months(tmpa.COMM_MTH,-1) = trunc(tmpb.trx_month,'MM')
 where tmpa.IS_PLAN_CD = 'Y'
        -- and tmpa.Payment_flg = 'ICT_CONTRACTED'
 group by tmpa.RPT_MTH,tmpa.COMM_MTH,tmpa.CASE_ID
)tmpb
on tmpa.CASE_ID = tmpb.CASE_ID
left outer join
(select tmpa.cust_num,tmpa.subr_num,tmpa.case_id,tmpa.bill_start_date,
       nvl(max(tmpb.INV_TOT) keep (dense_rank first order by tmpb.INV_DATE asc),0) as NP_TARIFF,  --?
       nvl(max(tmpb.INV_NUM) keep (dense_rank first order by tmpb.INV_DATE asc,tmpb.INV_TOT desc),' ') as BILL_REFERENCE  --?
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T tmpa
left outer join prd_adw.inv_header tmpb
on tmpa.cust_num = tmpb.cust_num
   and tmpa.subr_num = tmpb.subr_num
   and tmpb.inv_date > tmpa.bill_start_date
   and tmpb.inv_date between tmpa.bill_start_date and tmpa.bill_start_date + 180
where tmpa.Payment_flg = 'ICT_ONEOFF'
group by tmpa.cust_num,tmpa.subr_num,tmpa.case_id,tmpa.bill_start_date
)tmpc
on tmpa.case_id = tmpc.case_id;

commit;
--------------------------------------------------------------------------------------------------------


insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_003_T  --667
(
         rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,payment_flg
        ,contract_mth
        ,bill_reference
        ,np_tariff
        ,np_rebate
        ,np_total
        ,json_rmk
        ,ict_type1
        ,ict_type2
        ,ict_type3
        ,total_cost_amt
        ,comm_rate
        ,comm_amt
        ,create_ts
        ,refresh_ts
)
select
    tmpa.RPT_MTH,
    tmpa.COMM_MTH,
    tmpa.CASE_ID,
    tmpa.CUST_NUM,
    tmpa.SUBR_NUM,
    tmpa.Payment_flg,
    tmpa.Contract_mth,
    tmpa.BILL_REFERENCE,
    tmpa.NP_TARIFF,
    tmpa.NP_REBATE,
    (tmpa.NP_TARIFF + tmpa.NP_REBATE) * tmpa.Contract_mth as NP_TOTAL,
    '"' || 'NP_TARIFF' || '":"' || tmpa.NP_TARIFF || '","' || 'NP_REBATE' || '":"' || tmpa.NP_REBATE || '"' as JSON_RMK,
    tmpb.ICT_TYPE1,
    tmpb.ICT_TYPE2,
    tmpb.ICT_TYPE3,
    tmpc.TOTAL_COST_AMT,
    case when trunc(tmpb.MIN_SUBR_SW_ON_DATE,'mm') = tmpa.RPT_MTH
         then 0.06
         else 0.03
    end as COMM_RATE,
    case when trunc(tmpb.MIN_SUBR_SW_ON_DATE,'mm') = tmpa.RPT_MTH
         then ((tmpa.NP_TARIFF + tmpa.NP_REBATE ) * tmpa.Contract_mth - tmpc.TOTAL_COST_AMT ) * 0.06
         else ((tmpa.NP_TARIFF + tmpa.NP_REBATE ) * tmpa.Contract_mth - tmpc.TOTAL_COST_AMT ) * 0.03
    end as COMM_AMT,
    sysdate,
    sysdate
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_003A01_T tmpa
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T tmpb
on tmpa.case_id = tmpb.case_id
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002_T tmpc
on tmpa.case_id = tmpc.case_id;

commit;

--------------------------------------------------------------------------------------------------------
insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_004_T  --667
(
         rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,bill_serv_cd
        ,bill_start_date
        ,bill_end_date
        ,salesman_cd
        ,dealer_cd
        ,bill_rate
        ,ict_type1
        ,ict_type2
        ,ict_type3
        ,is_plan_cd
        ,subr_sw_on_date
        ,subr_sw_off_date
        ,rate_plan_cd
        ,subr_stat_cd
        ,vendor
        ,ld_inv_num
        ,ld_start_date
        ,ld_expired_date
        ,ld_mkt_cd
        ,ld_cd
        ,payment_flg
        ,contract_mth
        ,cost_src
        ,total_cost_amt
        ,bill_reference
        ,np_tariff
        ,np_rebate
        ,np_total
        ,comm_rate
        ,comm_amt
        ,json_rmk
        ,create_ts
        ,refresh_ts
        ,contract_type
        ,skip_flg
        ,map_acct_mgr
        ,map_team_head
        ,min_subr_sw_on_date
)
select
    tmpa.RPT_MTH,
    tmpa.COMM_MTH,
    tmpa.CASE_ID,
    tmpa.CUST_NUM,
    tmpa.SUBR_NUM,
    tmpb.BILL_SERV_CD,
    tmpb.BILL_START_DATE,
    tmpb.BILL_END_DATE,
    tmpb.SALESMAN_CD,
    tmpb.DEALER_CD,
    tmpb.BILL_RATE,
    tmpb.ICT_TYPE1,
    tmpb.ICT_TYPE2,
    tmpb.ICT_TYPE3,
    tmpb.IS_PLAN_CD,
    tmpb.SUBR_SW_ON_DATE,
    tmpb.SUBR_SW_OFF_DATE,
    tmpb.RATE_PLAN_CD,
    tmpb.SUBR_STAT_CD,
    tmpb.VENDOR,
    tmpb.LD_INV_NUM,
    tmpb.LD_START_DATE,
    tmpb.LD_EXPIRED_DATE,
    tmpb.LD_MKT_CD,
    tmpb.LD_CD,
    tmpa.PAYMENT_FLG,
    tmpa.CONTRACT_MTH,
    tmpc.COST_SRC,
    tmpa.TOTAL_COST_AMT,
    tmpa.BILL_REFERENCE,
    tmpa.NP_TARIFF,
    tmpa.NP_REBATE,
    TMPA.NP_TOTAL,
    tmpa.COMM_RATE,
    TMPA.COMM_AMT,
    nvl(tmpa.json_rmk,' ')|| nvl(tmpb.json_rmk,' ')|| nvl(tmpc.json_rmk,' ') as JSON_RMK,
    sysdate,
    sysdate,
    case when trunc(tmpb.MIN_SUBR_SW_ON_DATE,'mm') = tmpa.RPT_MTH
         then 'NEW_ACTV'
         else 'RENEW'
    end as CONTRACT_TYPE,
    tmpb.skip_flg,
    tmpb.map_acct_mgr,
    tmpb.map_team_head,
    tmpb.min_subr_sw_on_date
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_003_T tmpa
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001_T tmpb
on tmpa.case_id = tmpb.case_id
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_002_T tmpc
on tmpa.case_id = tmpc.case_id;


commit;


--------------------------------------------------------------------------------------------------------
insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_005_T
(
         comm_mth
        ,case_id
        ,rpt_mth
        ,subr_num
        ,cust_name
        ,cust_num
        ,idbr
        ,subr_sw_on_date
        ,subr_sw_off_date
        ,account_mgr
        ,team_head
        ,ict_type1
        ,ict_type2
        ,ict_type3
        ,bill_serv_cd
        ,bill_rate
        ,bill_start_date
        ,min_bill_start_date
        ,bill_end_date
        ,ah_prod_cd
        ,orafin_prod_cd
        ,np_rebate
        ,bill_amt
        ,contract_mth
        ,ld_cd
        ,np_total
        ,vendor
        ,total_cost_amt
        ,gross_profit
        ,comm_rate
        ,comm_amt
        ,bill_reference
        ,np_tariff
        ,ah_prod_cd_inv_lst
        ,contract_type
        ,is_plan_cd
        ,json_rmk
        ,payment_flg
        ,ld_inv_num
        ,ld_start_date
        ,ld_expired_date
        ,ld_mkt_cd
        ,subr_stat_cd
        ,create_ts
        ,refresh_ts
        ,skip_flg
        ,min_subr_sw_on_date
)
select tmpa.comm_mth,
       tmpa.case_id,
       tmpa.rpt_mth,
       tmpa.subr_num,
       nvl(cih.CUST_NAME,' ') as cust_name,
       tmpa.cust_num,
       nvl(cih.HKID_BR_PREFIX,' ') as IDBR,
       tmpa.SUBR_SW_ON_DATE,
       tmpa.SUBR_SW_OFF_DATE,
       --nvl(acmgr.ACCOUNT_MGR,' ') as ACCOUNT_MGR,
       --nvl(acmgr.TEAM_HEAD,' ') as TEAM_HEAD,
       tmpa.map_acct_mgr as account_mgr, --nvl(bmstfli.ACCT_MGR_FULLNAME,' ') as ACCOUNT_MGR,
       tmpa.map_team_head as team_head, --nvl(bmstfli.TEAM_HEAD,' ') as TEAM_HEAD,
       tmpa.ICT_TYPE1,
       tmpa.ICT_TYPE2,
       tmpa.ICT_TYPE3,
       tmpa.bill_serv_cd,
       tmpa.BILL_RATE,
       tmpa.bill_start_date,
       case when tmpb.bill_start_date is not null
            then tmpb.bill_start_date
            else tmpa.bill_start_date
       end as min_bill_Start_date,
       tmpa.bill_end_date,
       ' ' as AH_PROD_CD,
       ' ' as orafin_prod_cd,
       tmpa.NP_REBATE,
       (tmpa.NP_TARIFF + tmpa.NP_REBATE) as bill_amt,
       tmpa.CONTRACT_MTH,
       tmpa.LD_CD,
       tmpa.NP_TOTAL,
       tmpa.VENDOR,
       tmpa.TOTAL_COST_AMT,
       (tmpa.NP_TOTAL - tmpa.TOTAL_COST_AMT) as Gross_Profit,
       tmpa.COMM_RATE,
       tmpa.COMM_AMT,
       tmpa.BILL_REFERENCE,
       case when tmpa.Payment_flg = 'ICT_CONTRACTED'
            then 0
            else tmpa.NP_TARIFF
       end as NP_TARIFF,
       ' ' as AH_PROD_CD_INV_LST,
       tmpa.CONTRACT_TYPE,
       tmpa.IS_PLAN_CD,
       tmpa.JSON_RMK ||
            case when tmpa.bill_start_date = tmpb.bill_start_date
                 then ',"EXCEPT_RMK":"EXCEPT_DUPLICATE_BILL_CD"'
                 else ''
            end ||
            case when tmpa.COST_SRC <> ' '
                 then ',"COST_SRC":"' || tmpa.COST_SRC || '"'
                 else ''
            end || ',"ONEOFF_COST":"' || bmict.ONEOFF_COST
                || '","ADD_ONEOFF_COST":"' ||bmict.ADD_ONEOFF_COST
                || '","MONTHLY_COST":"' || bmict.MONTHLY_COST || '"'
       as JSON_RMK,
       tmpa.PAYMENT_FLG,
       tmpa.LD_INV_NUM,
       tmpa.LD_START_DATE,
       tmpa.LD_EXPIRED_DATE,
       tmpa.LD_MKT_CD,
       tmpa.SUBR_STAT_CD,
       sysdate as CREATE_TS,
       sysdate as REFRESH_TS,
       case when tmpa.bill_start_date = tmpb.bill_start_date
                then 'SKIP_DUPLICATE_FOUND;'||tmpa.skip_flg
        else tmpa.skip_flg
        end skip_flg,
       tmpa.min_subr_sw_on_date
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_004_T tmpa
left outer join
(select cust_num,subr_num,bill_serv_cd,
        max(tmpa.bill_start_date) keep (dense_rank first order by tmpa.bill_start_date asc) as bill_start_date,
        count(*)
        from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_004_T tmpa
        group by cust_num,subr_num,bill_serv_cd
        having count(*) > 1) tmpb
on tmpa.cust_num = tmpb.cust_num
   and tmpa.subr_num = tmpb.subr_num
   and tmpa.bill_serv_cd = tmpb.bill_serv_cd
left outer join prd_adw.CUST_INFO_HIST cih
    on tmpa.cust_num = cih.cust_num
       and &rpt_e_date between cih.start_date and cih.end_date
       and cih.cust_stat_cd = 'OK'
left outer join ${etlvar::ADWDB}.BM_COMM_RPT_ICT_REF bmict
    on tmpa.BILL_SERV_CD = bmict.BILL_CD;

commit;

--------------------------------------------------------

insert into ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_006_T
(
         comm_mth
        ,case_id
        ,rpt_mth
        ,subr_num
        ,cust_name
        ,cust_num
        ,idbr
        ,subr_sw_on_date
        ,subr_sw_off_date
        ,account_mgr
        ,team_head
        ,ict_type1
        ,ict_type2
        ,ict_type3
        ,bill_serv_cd
        ,bill_rate
        ,bill_start_date
        ,min_bill_start_date
        ,bill_end_date
        ,ah_prod_cd
        ,orafin_prod_cd
        ,np_rebate
        ,bill_amt
        ,contract_mth
        ,ld_cd
        ,np_total
        ,vendor
        ,total_cost_amt
        ,gross_profit
        ,comm_rate
        ,comm_amt
        ,bill_reference
        ,np_tariff
        ,ah_prod_cd_inv_lst
        ,contract_type
        ,is_plan_cd
        ,json_rmk
        ,payment_flg
        ,ld_inv_num
        ,ld_start_date
        ,ld_expired_date
        ,ld_mkt_cd
        ,subr_stat_cd
        ,skip_flg
        ,min_subr_sw_on_date
        ,BILL_SALESMAN_CD
        ,LD_SALESMAN_CD
        ,FINAL_BILL_SERV_CD
        ,FINAL_BILL_START_DATE
        ,FINAL_BILL_END_DATE
)
select
         tmpa.comm_mth
        ,tmpa.case_id
        ,tmpa.rpt_mth
        ,tmpa.subr_num
        ,tmpa.cust_name
        ,tmpa.cust_num
        ,tmpa.idbr
        ,tmpa.subr_sw_on_date
        ,tmpa.subr_sw_off_date
        ,tmpa.account_mgr
        ,tmpa.team_head
        ,tmpa.ict_type1
        ,tmpa.ict_type2
        ,tmpa.ict_type3
        ,tmpa.bill_serv_cd
        ,tmpa.bill_rate
        ,tmpa.bill_start_date
        ,tmpa.min_bill_start_date
        ,tmpa.bill_end_date
        ,tmpa.ah_prod_cd
        ,tmpa.orafin_prod_cd
        ,tmpa.np_rebate
        ,tmpa.bill_amt
        ,tmpa.contract_mth
        ,tmpa.ld_cd
        ,tmpa.np_total
        ,tmpa.vendor
        ,tmpa.total_cost_amt
        ,tmpa.gross_profit
        ,tmpa.comm_rate
        ,tmpa.comm_amt
        ,tmpa.bill_reference
        ,tmpa.np_tariff
        ,tmpa.ah_prod_cd_inv_lst
        ,tmpa.contract_type
        ,tmpa.is_plan_cd
        ,tmpa.json_rmk
        ,tmpa.payment_flg
        ,tmpa.ld_inv_num
        ,tmpa.ld_start_date
        ,tmpa.ld_expired_date
        ,tmpa.ld_mkt_cd
        ,tmpa.subr_stat_cd
        ,tmpa.skip_flg
        ,tmpa.min_subr_sw_on_date
        ,tmpb.bill_salesman_cd
        ,tmpb.ld_salesman_cd
        ,nvl(tmpc.BILL_SERV_CD,' ') as FINAL_BILL_SERV_CD
        ,nvl(tmpc.BILL_START_DATE,to_date('29991231','yyyymmdd')) as FINAL_BILL_START_DATE
        ,nvl(tmpc.BILL_END_DATE,to_date('29991231','yyyymmdd')) as FINAL_BILL_END_DATE
from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_005_T tmpa
left outer join ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_001C01_T tmpb
    on tmpa.case_id = tmpb.case_id
left outer join (select tmpa.case_id,bs.bill_serv_cd,bs.BILL_START_DATE,bs.BILL_END_DATE
                        from ${etlvar::TMPDB}.B_BM_ICT_COMM_RPT_005_T tmpa
                 cross join prd_adw.bill_servs bs
                 where tmpa.subr_num = bs.subr_num
                       and tmpa.bill_serv_cd = bs.bill_serv_cd
                       and &comm_mth -1 between bs.bill_start_date and bs.bill_end_date) tmpc
on tmpa.case_id = tmpc.case_id;
commit;

---------------------------------------------------------


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















