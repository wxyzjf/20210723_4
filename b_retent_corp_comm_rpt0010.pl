######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29 
#
#
######################################################


my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
#my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_B_RETENT_CORP_COMM_RPT/bin/master_dev.pl";
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

set define on;
set linesize 2000
alter session force parallel query parallel 30;
alter session force parallel dml parallel 30;

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_001A_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_001A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_001A02_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_001A03_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_001A04_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_001A05_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_001B_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_001_T');
-----

set define on;
define tx_date = trunc(to_date('$etlvar::TXDATE','YYYY-MM-DD'),'MM');
define comm_mth = add_months(&tx_date,-1);
define rpt_mth = add_months(&comm_mth,-3);
define rpt_s_date = add_months(&comm_mth,-3);
define rpt_e_date = add_months(&comm_mth,-2)-1;


prompt 'b_retentcopr_comm_001A01_T [Prepare pos invoice ]';

insert into ${etlvar::TMPDB}.b_retent_corp_comm_001A01_t
(
     RPT_MTH
    ,COMM_MTH
    ,CASE_ID
    ,CUST_NUM
    ,SUBR_NUM
    ,POS_INV_NUM
    ,POS_MKT_CD
    ,POS_LD_CD
    ,POS_INV_DATE
    ,POS_INV_TAG
    ,JSON_RMK
) select 
     &rpt_s_date as RPT_MTH
    ,&comm_mth as COMM_MTH
    ,i.inv_num ||'-'||i.subr_num CASE_ID
    ,i.CUST_NUM
    ,i.SUBR_NUM
    ,i.inv_num as POS_INV_NUM
    ,i.mkt_cd as POS_MKT_CD
    ,i.ld_cd as POS_LD_CD
    ,i.inv_date as POS_INV_DATE
    ,' ' as POS_INV_TAG
    ,' ' as JSON_RMK
from prd_adw.pos_inv_header i
where i.inv_date between &rpt_s_date and &rpt_e_date
-- and salesman_cd in('G08038','G10053','G16166','G18126','G15042','G10064','G14148','G18153','G19018'
--                 ,'G19108','G12324','G14283','G15111','G17017','CA94040','CA94260','CA15200'
--                 ,'CA17141','CA18121','CA19079')
 and salesman_cd in(select SALESMAN_CD from ${etlvar::ADWDB}.retent_corp_comm_salesman
                    where COMM_MTH = &comm_mth)
 and i.inv_num not in(
        select r.inv_num from prd_adw.pos_return_header r
        where r.trx_date between &rpt_s_date and &rpt_e_date
)
 and i.ld_cd <> ' ';
commit;

prompt 'b_retentcopr_comm_001A02_T [Prepare cstpn cases ]';
insert into ${etlvar::TMPDB}.b_retent_corp_comm_001A02_t
(
     RPT_MTH
    ,COMM_MTH
    ,CASE_ID
    ,CUST_NUM
    ,SUBR_NUM
    ,POS_INV_NUM
    ,POS_MKT_CD
    ,POS_LD_CD
    ,POS_INV_DATE
    ,POS_INV_TAG
    ,JSON_RMK
)
----E01 offer with CSTPN with LD CODE
select     
     t.RPT_MTH
    ,t.COMM_MTH
    ,t.CASE_ID
    ,t.CUST_NUM
    ,t.SUBR_NUM
    ,t.POS_INV_NUM
    ,t.POS_MKT_CD
    ,t.POS_LD_CD
    ,t.POS_INV_DATE
    ,'CSTPN_LD'  POS_INV_TAG
    ,t.JSON_RMK
from ${etlvar::TMPDB}.b_retent_corp_comm_001A01_t t
where t.pos_mkt_cd ='CSTPN' and t.pos_ld_cd <> ' '
union all 
----CSTPN offer with mass offer
Select   
        t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.cust_num
        ,t.subr_num
        ,t2.pos_inv_num as mass_inv
        ,t2.pos_mkt_cd
        ,t2.pos_ld_cd
        ,t2.pos_inv_date
        ,'CSTPN_MASS' POS_INV_TAG
        ,t.json_rmk || ',"CSTPN_INV"="'||t.pos_inv_num||'"' as JSON_RMK
    from ${etlvar::TMPDB}.b_retent_corp_comm_001A01_t t
        ,${etlvar::TMPDB}.b_retent_corp_comm_001A01_t t2
        ,prd_adw.mkt_ref_vw mk
    where t.pos_mkt_cd ='CSTPN' 
      and t.pos_ld_cd =' '
      and t.cust_num = t2.cust_num
      and t.subr_num = t2.subr_num
      and t.pos_inv_date = t2.pos_inv_date
      and t2.pos_mkt_cd <>'CSTPN'
      and t2.pos_mkt_cd = mk.mkt_cd
      and mk.ld_revenue='P' ;
commit;

prompt 'b_retentcopr_comm_001A02_T [ Prepare non cstpn normal mass cases]';
insert into ${etlvar::TMPDB}.b_retent_corp_comm_001A02_t
(
     RPT_MTH
    ,COMM_MTH
    ,CASE_ID
    ,CUST_NUM
    ,SUBR_NUM
    ,POS_INV_NUM
    ,POS_MKT_CD
    ,POS_LD_CD
    ,POS_INV_DATE
    ,POS_INV_TAG
    ,JSON_RMK
)
Select
        t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.cust_num
        ,t.subr_num
        ,t.pos_inv_num
        ,t.pos_mkt_cd
        ,t.pos_ld_cd
        ,t.pos_inv_date
        ,'NORMAL_MASS' POS_INV_TAG
        ,t.json_rmk
    from 
        ${etlvar::TMPDB}.b_retent_corp_comm_001A01_t t
        ,prd_adw.mkt_ref_vw mk
    where
       t.pos_mkt_cd <>'CSTPN'
      and t.pos_mkt_cd = mk.mkt_cd
      --and mk.ld_revenue in ('P','V')
      and t.pos_ld_cd <> ' '
      and t.pos_inv_num not in (select pos_inv_num from ${etlvar::TMPDB}.b_retent_corp_comm_001A02_t)
      and t.pos_mkt_cd in (Select mkt_cd from ${etlvar::ADWDB}.RETENT_CORP_COMM_MKT_REF where COMM_MTH = &comm_mth);
commit;

prompt 'b_retentcopr_comm_001A02_T [ Prepare override normal mass cases]';
insert into ${etlvar::TMPDB}.b_retent_corp_comm_001A02_t
(    
     RPT_MTH
    ,COMM_MTH
    ,CASE_ID
    ,CUST_NUM
    ,SUBR_NUM
    ,POS_INV_NUM
    ,POS_MKT_CD
    ,POS_LD_CD
    ,POS_INV_DATE
    ,POS_INV_TAG
    ,JSON_RMK
)
Select
        t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.cust_num
        ,t.subr_num
        ,t.pos_inv_num
        ,case when ov.overr_mkt_cd <> ' ' then ov.overr_mkt_cd else t.pos_mkt_cd end pos_mkt_cd
        ,case when ov.overr_ld_cd <> ' ' then ov.overr_ld_cd else t.pos_ld_cd end  pos_ld_cd
        ,t.pos_inv_date
        ,'NORMAL_MASS' POS_INV_TAG
        ,t.json_rmk
    from
        ${etlvar::TMPDB}.b_retent_corp_comm_001A01_t t
        ,${etlvar::ADWDB}.RETENT_CORP_COMM_OVERR_HIST ov
    where
       t.pos_inv_num = ov.inv_num
      and t.pos_ld_cd <> ' '
      --and t.pos_inv_num not in (select pos_inv_num from ${etlvar::TMPDB}.b_retent_corp_comm_001A02_t)
      and t.comm_mth = ov.comm_mth;
commit;

prompt 'b_retentcopr_comm_001A03_T [Prepare cstpn mass cases and map to orignal one  pos invoice ]';
insert into ${etlvar::TMPDB}.b_retent_corp_comm_001A03_t
(
     RPT_MTH
    ,COMM_MTH
    ,CASE_ID
    ,CUST_NUM
    ,SUBR_NUM
    ,POS_INV_NUM
    ,POS_MKT_CD
    ,POS_LD_CD
    ,POS_INV_DATE
    ,POS_INV_TAG
    ,JSON_RMK
)
select
     t.RPT_MTH
    ,t.COMM_MTH
    ,t.CASE_ID
    ,t.CUST_NUM
    ,t.SUBR_NUM
    ,t.POS_INV_NUM
    ,t.POS_MKT_CD
    ,t.POS_LD_CD
    ,t.POS_INV_DATE
    ,t.POS_INV_TAG
    ,t.JSON_RMK
from ${etlvar::TMPDB}.b_retent_corp_comm_001A02_t t ;
commit;

prompt 'b_retent_corp_comm_001A04_T [Map coressponding LD by pos  invoice number ]';
----Part one map by pos invoice---
insert into ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001A04_T
(    rpt_mth
    ,comm_mth
    ,case_id
    ,cust_num
    ,subr_num
    ,pos_inv_num
    ,pos_mkt_cd
    ,pos_inv_date
    ,pos_inv_tag
    ,pos_ld_cd
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_start_date
    ,ld_expired_date
    ,json_rmk)
select
     t.rpt_mth
    ,t.comm_mth
    ,t.case_id
    ,t.cust_num
    ,t.subr_num
    ,t.pos_inv_num
    ,t.pos_mkt_cd
    ,t.pos_inv_date
    ,t.pos_inv_tag
    ,t.pos_ld_cd
    ,sl.inv_num ld_inv_num
    ,sl.ld_cd ld_cd
    ,sl.mkt_cd ld_mkt_cd
    ,sl.ld_start_date
    ,sl.ld_expired_date
    ,t.json_rmk    
from  ${etlvar::TMPDB}.b_retent_corp_comm_001A03_t t
     ,prd_adw.subr_ld_hist sl
where t.pos_inv_num = sl.inv_num
 and t.comm_mth -1 between sl.start_date and sl.end_date;
commit;

prompt 'b_retent_corp_comm_001A04_T [Map coressponding LD month end ]';
----Part two map by next 2 months new ld with same ld code---
insert into ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001A04_T
(    rpt_mth
    ,comm_mth
    ,case_id
    ,cust_num
    ,subr_num
    ,pos_inv_num
    ,pos_mkt_cd
    ,pos_inv_date
    ,pos_inv_tag
    ,pos_ld_cd
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_start_date
    ,ld_expired_date
    ,json_rmk)
select
     t.rpt_mth
    ,t.comm_mth
    ,t.case_id
    ,t.cust_num
    ,t.subr_num
    ,t.pos_inv_num
    ,t.pos_mkt_cd
    ,t.pos_inv_date
    ,t.pos_inv_tag
    ,t.pos_ld_cd
    ,max(sl.inv_num) keep(dense_rank first order by sl.ld_start_date) ld_inv_num
    ,max(sl.ld_cd) keep(dense_rank first order by sl.ld_start_date)   ld_cd
    ,max(sl.mkt_cd) keep(dense_rank first order by sl.ld_start_date)  ld_mkt_cd
    ,max(sl.ld_start_date) keep(dense_rank first order by sl.ld_start_date) 
    ,max(sl.ld_expired_date) keep(dense_rank first order by sl.ld_start_date) 
    ,t.json_rmk||',"TAG1A04"="MAP_LATEST_LD"'
from  ${etlvar::TMPDB}.b_retent_corp_comm_001A03_t t
     ,prd_adw.subr_ld_hist sl
where t.cust_num = sl.cust_num
  and t.subr_num = sl.subr_num
  and add_months(t.rpt_mth,3) - 1 between sl.start_date and sl.end_date
  and sl.ld_start_date between t.pos_inv_date and  add_months(t.rpt_mth,3) - 1
  and t.pos_ld_cd = sl.ld_cd
  and t.case_id not in ( select r.case_id from ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001A04_T r)
group by 
     t.rpt_mth
    ,t.comm_mth
    ,t.case_id
    ,t.cust_num
    ,t.subr_num
    ,t.pos_inv_num
    ,t.pos_mkt_cd
    ,t.pos_inv_date
    ,t.pos_inv_tag
    ,t.pos_ld_cd
    ,t.json_rmk;
commit;

prompt 'b_retent_corp_comm_001A04_T [Map those unmap LD month end ]';
insert into ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001A04_T
(    rpt_mth
    ,comm_mth
    ,case_id
    ,cust_num
    ,subr_num
    ,pos_inv_num
    ,pos_mkt_cd
    ,pos_inv_date
    ,pos_inv_tag
    ,pos_ld_cd
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_start_date
    ,ld_expired_date
    ,json_rmk)
select
     t.rpt_mth
    ,t.comm_mth
    ,t.case_id
    ,t.cust_num
    ,t.subr_num
    ,t.pos_inv_num
    ,t.pos_mkt_cd
    ,t.pos_inv_date
    ,t.pos_inv_tag
    ,t.pos_ld_cd
    ,' ' ld_inv_num
    ,t.pos_ld_cd as ld_cd
    ,t.pos_mkt_cd as ld_mkt_cd
    ,t.pos_inv_date as ld_start_date
    ,date '2999-12-31' as  ld_expired_date
    ,t.json_rmk||',"TAG1A04"="UNMAP_LD"'
from  ${etlvar::TMPDB}.b_retent_corp_comm_001A03_t t
where  t.case_id not in ( select r.case_id from ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001A04_T r);
commit;
prompt 'b_retent_corp_comm_001A_t [Preparing base profile and override the ld_expired_date to day one ]'
insert into ${etlvar::TMPDB}.b_retent_corp_comm_001A_t
(   
        rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,pos_inv_num
        ,pos_mkt_cd
        ,pos_inv_date
        ,pos_inv_tag
        ,pos_ld_cd
        ,ld_inv_num
        ,ld_cd
        ,ld_mkt_cd
        ,ld_start_date
        ,ld_expired_date
        ,bill_cycle
        ,subr_sw_on_date
        ,subr_stat_cd
        ,rate_plan_cd
        ,json_rmk
)
select
        t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.cust_num
        ,t.subr_num
        ,t.pos_inv_num
        ,t.pos_mkt_cd
        ,t.pos_inv_date
        ,t.pos_inv_tag
        ,t.pos_ld_cd
        ,t.ld_inv_num
        ,t.ld_cd
        ,t.ld_mkt_cd
        ,t.ld_start_date
        ,nvl(sl.ld_expired_date,t.ld_expired_date)
        ,nvl(s.bill_cycle,0) as bill_cycle
        ,nvl(s.subr_sw_on_date,date '2999-12-31') subr_sw_on_date
        ,nvl(s.subr_stat_cd,' ') subr_stat_cd
        ,nvl(s.rate_plan_cd,' ') rate_plan_cd
        ,t.json_rmk
from    ${etlvar::TMPDB}.b_retent_corp_comm_001A04_t t
left outer join prd_adw.subr_ld_hist sl
  on  t.ld_inv_num = sl.inv_num
  and t.ld_start_date between sl.start_date and sl.end_date
--- map profile by ld ---
left outer join prd_adw.subr_info_hist s
  on  sl.cust_num = s.cust_num
  and sl.subr_num =s.subr_num
  and sl.ld_start_date between s.start_date and s.end_date;
commit;
prompt

prompt 'b_retent_corp_comm_001A05_t [Override the subr_on_date]';

insert into ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001A05_T
    (   case_id
        ,overr_subr_sw_on_date
        )
 Select t.case_id,s.subr_sw_on_date as overr_subr_sw_on_date
 from ${etlvar::TMPDB}.b_retent_corp_comm_001A_t t
     ,prd_adw.subr_info_hist s   
 where t.subr_sw_on_date between &rpt_s_date and &rpt_e_date
   and s.subr_num = t.subr_num
   and s.cust_num <> t.cust_num
   and t.subr_sw_on_date = s.subr_sw_off_date
   and s.subr_stat_cd='TX';
commit;

update ${etlvar::TMPDB}.b_retent_corp_comm_001A_t t
set t.subr_sw_on_date
=(select min(r.overr_subr_sw_on_date) from ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001A05_T r where r.case_id = t.case_id)
where t.case_id in (
        select rr.case_id from ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001A05_T rr
);
commit;


prompt 'b_retent_corp_comm_001A05_t [Update the new vas mkt code cases.]';

--update ${etlvar::TMPDB}.b_retent_corp_comm_001A_T t
--   set t.pos_inv_tag ='SKIP_NEW_VAS'
-- where t.case_id in (
--  Select t.case_id 
--  from
--    ${etlvar::TMPDB}.b_retent_corp_comm_001A_t t 
--    left outer join prd_adw.mkt_ref_vw v
--            on t.pos_mkt_cd = v.mkt_cd 
--    left outer join prd_adw.subr_mkt_cd_info m
--            on  t.subr_num = m.subr_num            
--            and t.pos_mkt_cd = m.mkt_cd
--            and m.inv_date between add_months(&rpt_s_date,-24) and &rpt_s_date -1
--    and v.ld_revenue ='V'
--    where m.subr_num is null
-- );

--update ${etlvar::TMPDB}.b_retent_corp_comm_001A_T t
--   set t.pos_inv_tag ='SKIP_NEW_VAS'
-- where t.case_id in (
--  Select 
--    t.case_id  
--  From ${etlvar::TMPDB}.b_retent_corp_comm_001A_t t 
--    left outer join  ${etlvar::ADWDB}.RETENT_CORP_COMM_MKT_REF v
--        on t.pos_mkt_cd = v.mkt_cd 
--    left outer join prd_adw.subr_ld_hist m
--            on  t.subr_num = m.subr_num
--            and t.pos_mkt_cd = m.mkt_cd
--            and m.ld_start_date < &rpt_s_date
--            and m.ld_start_date >= add_months(&rpt_s_date ,-36)
--    where 
--      m.subr_num is  null
--    and upper(v.bm_cat) ='OTHERS'
----Select t.case_id  from ${etlvar::TMPDB}.b_retent_corp_comm_001A_t t
-- --   left outer join ${etlvar::ADWDB}.RETENT_CORP_COMM_MKT_REF  v
--  --          on t.pos_mkt_cd = v.mkt_cd
--   -- left outer join prd_adw.subr_mkt_cd_info m
--    --        on  t.subr_num = m.subr_num
--     --       and t.pos_mkt_cd = m.mkt_cd
--      --      and &rpt_s_date - 1 between m.eff_start_date and m.eff_end_date
--    --where m.subr_num is  null
--    --and upper(v.bm_cat) ='OTHERS'
-- ); 
 
commit;

prompt 'b_retent_corp_comm_001b_T [Preparing base profile ]';
insert into ${etlvar::TMPDB}.b_retent_corp_comm_001b_t
    (rpt_mth
    ,comm_mth
    ,case_id
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_start_date
    ,ld_expired_date
    ,old_ld_inv_num
    ,old_ld_cd
    ,old_ld_mkt_cd
    ,old_ld_start_date
    ,old_ld_expired_date
    ,old_rate_plan_cd
)
select 
     tt.rpt_mth
    ,tt.comm_mth
    ,tt.case_id
    ,tt.ld_inv_num
    ,tt.ld_cd
    ,tt.ld_mkt_cd
    ,tt.ld_start_date
    ,tt.ld_expired_date
    ,tt.old_ld_inv_num
    ,tt.old_ld_cd
    ,tt.old_ld_mkt_cd
    ,tt.old_ld_start_date
    ,nvl(osl.ld_expired_date,date '1900-01-01') as old_ld_expired_date
    ,nvl(op.rate_plan_cd,' ') as old_rate_plan_cd
from (
        select 
             t.rpt_mth
            ,t.comm_mth
            ,t.case_id
            ,t.ld_inv_num
            ,t.ld_cd
            ,t.ld_mkt_cd
            ,t.ld_start_date
            ,t.ld_expired_date
            ,max(o.inv_num) keep (dense_rank first order by o.ld_expired_date desc) as  old_ld_inv_num
            ,max(o.ld_cd) keep (dense_rank first order by o.ld_expired_date desc) as old_ld_cd
            ,max(o.mkt_cd) keep (dense_rank first order by o.ld_expired_date desc) as old_ld_mkt_cd
            ,max(o.ld_start_date) keep (dense_rank first order by o.ld_expired_date desc) as old_ld_start_date
            ,max(o.ld_expired_date) keep (dense_rank first order by o.ld_expired_date desc) as old_ld_expired_date
            ,max(o.subr_num) keep (dense_rank first order by o.ld_expired_date desc) as old_subr_num
            ,max(o.cust_num) keep (dense_rank first order by o.ld_expired_date desc) as old_cust_num
            ,max(o.end_date) keep (dense_rank first order by o.ld_expired_date desc) as old_map_profile_end_date
        from ${etlvar::TMPDB}.b_retent_corp_comm_001a_t t
            ,prd_adw.subr_ld_hist o
        where t.subr_num = o.subr_num
         and t.cust_num = o.cust_num
         --and t.ld_start_date > o.ld_expired_date
         ---- Suppose the old ld valid in last month
         and &rpt_s_date - 1  between o.ld_start_date and o.ld_expired_date
         and o.mkt_cd in (select mkt_cd from prd_adw.mkt_ref_vw v where ld_revenue='P')
         and t.ld_mkt_cd not in (select rr.mkt_cd from ${etlvar::ADWDB}.RETENT_CORP_COMM_MKT_REF rr where upper(rr.BM_CAT) = 'OHTERS' and rr.COMM_MTH = &comm_mth)
        group by t.rpt_mth
            ,t.comm_mth
            ,t.case_id
            ,t.ld_inv_num
            ,t.ld_cd
            ,t.ld_mkt_cd
            ,t.ld_start_date
            ,t.ld_expired_date
) tt
------get the first ld_expired date from ld
        left outer join prd_adw.subr_info_hist  op
        on tt.old_subr_num = op.subr_num
        and tt.old_cust_num = op.cust_num
        and tt.old_map_profile_end_date between op.start_date and op.end_date
        left outer join prd_adw.subr_ld_hist osl
        on tt.old_ld_inv_num = osl.inv_num
        and  tt.old_ld_start_date between osl.start_date and osl.end_date ;
commit;


insert into  ${etlvar::TMPDB}.b_retent_corp_comm_001_t
(
         rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,pos_inv_num
        ,pos_mkt_cd
        ,pos_inv_date
        ,pos_inv_tag
        ,pos_ld_cd
        ,pos_mkt_type
        ,ld_inv_num
        ,ld_cd
        ,ld_mkt_cd
        ,ld_start_date
        ,ld_expired_date
        ,old_ld_inv_num
        ,old_ld_cd
        ,old_ld_mkt_cd
        ,old_ld_start_date
        ,old_ld_expired_date
        ,old_rate_plan_cd
        ,old_bill_rate
        ,bill_cycle
        ,subr_sw_on_date
        ,subr_stat_cd
        ,rate_plan_cd
        ,bill_rate
        ,json_rmk
) select 
         t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.cust_num
        ,t.subr_num
        ,t.pos_inv_num
        ,t.pos_mkt_cd
        ,t.pos_inv_date
        ,t.pos_inv_tag
        ,t.pos_ld_cd
        ,case when v.ld_revenue not in ('V','O') and t.pos_mkt_cd <> 'CSTPN' then 'NON_PLAN_MKT' else 'PLAN_MKT' end pos_mkt_type
        ,t.ld_inv_num
        ,t.ld_cd
        ,t.ld_mkt_cd
        ,t.ld_start_date
        ,t.ld_expired_date
        ,nvl(o.old_ld_inv_num,' ')
        ,nvl(o.old_ld_cd,' ')
        ,nvl(o.old_ld_mkt_cd,' ')
        ,nvl(o.old_ld_start_date,date '1900-01-01')
        ,nvl(o.old_ld_expired_date,date '1900-01-01')
        ,nvl(o.old_rate_plan_cd,' ')
        ,nvl(ro.bill_rate,0)
        ,t.bill_cycle
        ,t.subr_sw_on_date
        ,t.subr_stat_cd
        ,t.rate_plan_cd
        ,nvl(rn.bill_rate,0)
        ,t.json_rmk
from  ${etlvar::TMPDB}.b_retent_corp_comm_001a_t t
left outer join ${etlvar::TMPDB}.b_retent_corp_comm_001b_t o
        on t.case_id = o.case_id
left outer join prd_adw.bill_serv_ref rn
        on t.rate_plan_cd = rn.bill_serv_cd
        and &rpt_e_date  between rn.eff_start_date and rn.eff_end_date
left outer join prd_adw.bill_serv_ref ro
        on t.rate_plan_cd = ro.bill_serv_cd
        and &rpt_e_date  between ro.eff_start_date and ro.eff_end_date
left outer join prd_adw.mkt_ref_vw v
        on t.pos_mkt_cd = v.mkt_cd
-----not sw on current month-----
where trunc(t.subr_sw_on_date,'MM') <> trunc(t.ld_start_date,'MM') 
  and t.subr_sw_on_date not between &rpt_s_date and &rpt_e_date;
--  and t.pos_inv_tag <>'SKIP_NEW_VAS';
commit;



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

