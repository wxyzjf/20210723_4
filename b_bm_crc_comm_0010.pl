###################################################
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

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_001A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_001A02_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_001A03_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_001A04_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_001B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_001_T');

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
prompt 'Step B_BM_CRC_COMM_001A01_T : [A. E-01 SIM Only Offer -renew with ld ] ';


insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T
(
    trx_mth
   ,comm_mth
   ,case_id
   ,case_src
   ,ld_inv_num
   ,ld_cust_num
   ,ld_subr_num
   ,ld_start_date
   ,json_rmk
   ,create_ts
   ,refresh_ts
)
select  
        &trx_mth
        ,&comm_mth
        ,'CRCCLD-'||sl.inv_num ||'-' ||to_char(sl.start_date,'yymm') as case_id
        ,'SRC_RENEW_LD'
        ,sl.inv_num as ld_inv_num
        ,sl.cust_num as ld_cust_num
        ,sl.subr_num as ld_subr_num
        ,sl.ld_start_date
        ,',NA' as json_rmk
        ,sysdate
        ,sysdate
  from prd_adw.subr_ld_hist sl
      ,prd_adw.pos_inv_header p
where &comm_mth - 1 between sl.start_date and sl.end_date
  and sl.ld_start_date between &trx_s_date and add_months(&trx_s_date,4) -1
  and sl.ld_expired_date > &comm_mth - 1
  and sl.inv_num = p.inv_num
  and sl.void_flg <> 'Y'
  and sl.waived_flg <> 'Y'
  and sl.billed_flg<> 'Y' 
  and p.mkt_cd = 'RENEW'
  and p.ld_cd <> ' '
  and p.inv_num not in (
    select inv_num 
      from prd_adw.pos_return_header
     where trx_date between add_months(&trx_s_date,-24) and &comm_mth - 1
  );
commit;

prompt 'Step B_BM_CRC_COMM_001A01_T : [B. Standard SIM Only offer -renew with plan mkt] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T
(
    trx_mth
   ,comm_mth
   ,case_id
   ,case_src
   ,ld_inv_num
   ,ld_cust_num
   ,ld_subr_num
   ,ld_start_date
   ,json_rmk
   ,create_ts
   ,refresh_ts
) 
select  
---- distinct for mapping multiple renew ----
        distinct 
        &trx_mth
        ,&comm_mth
        ,'CRCCLD-'||sl.inv_num ||'-' ||to_char(sl.start_date,'yymm') as case_id
        ,'SRC_RENEW_MASS'
        ,sl.inv_num
        ,sl.cust_num as ld_cust_num
        ,sl.subr_num as ld_subr_num
        ,sl.ld_start_date
        ,',"RENEW_POS_INV":"'||p2.inv_num ||'"' as json_rmk
        ,sysdate
        ,sysdate
from prd_adw.subr_ld_hist sl
      ,prd_adw.pos_inv_header p2
      ,prd_adw.pos_inv_header p      
where &comm_mth -1 between sl.start_date and sl.end_date
  and sl.ld_start_date between &trx_s_date and add_months(&trx_s_date,4) -1
  and sl.ld_expired_date > &comm_mth
  and sl.inv_num = p2.inv_num
  and sl.void_flg <> 'Y'
  and sl.waived_flg <> 'Y'
  and sl.billed_flg<> 'Y' 
  and p.mkt_cd = 'RENEW'
  and p.ld_cd = ' '
  and p.case_id <> ' '  
---- for performance---
  and p.inv_date >= add_months(&trx_s_date ,-24)
----and p.case_id = p2.case_id
  and months_between (trunc(p.inv_date,'mm'),trunc(p2.inv_date,'mm')) between -1 and 1
  and p.cust_num = p2.cust_num
  and p.subr_num = p2.subr_num
---- for performance---
  and p2.inv_date >= add_months(&trx_s_date ,-24)
  and p2.ld_cd <> ' '
  and p2.case_id <> ' '
  and p2.inv_num not in(
    select inv_num 
      from prd_adw.pos_return_header
     where trx_date between add_months(&trx_s_date,-24) and &comm_mth - 1)
  and p2.inv_num not in (
        select ld_inv_num from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T
  ) ;
commit;

prompt 'Step B_BM_CRC_COMM_001A01_T : [C.Handset Bundle SIM Offer (via Invoicing): invoice with HS ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T
(
    trx_mth
   ,comm_mth
   ,case_id
   ,case_src
   ,ld_inv_num
   ,ld_cust_num
   ,ld_subr_num
   ,ld_start_date
   ,json_rmk
   ,create_ts
   ,refresh_ts)
select
        &trx_mth
        ,&comm_mth
        ,'CRCCLD-'||sl.inv_num ||'-' ||to_char(sl.start_date,'yymm') as case_id
        ,'SRC_RENEW_HS'
        ,sl.inv_num
        ,sl.cust_num as ld_cust_num
        ,sl.subr_num as ld_subr_num
        ,sl.ld_start_date
        ,',NA' as json_rmk 
        ,sysdate
        ,sysdate
  from prd_adw.subr_ld_hist sl
      ,prd_adw.pos_inv_header p        
where &comm_mth -1 between sl.start_date and sl.end_date
  and sl.inv_num = p.inv_num
  --and p.salesman_cd like 'CA%'
  and sl.ld_start_date between &trx_s_date and add_months(&trx_s_date,4) -1
  and sl.ld_expired_date > &comm_mth  
  and sl.void_flg <> 'Y'
  and sl.waived_flg <> 'Y'
  and sl.billed_flg <> 'Y'
  and sl.inv_num in (
    select distinct d.inv_num
    from prd_adw.pos_inv_detail d
    where d.inv_date between add_months(&trx_s_date,-24) and &comm_mth - 1
      and d.warehouse='AH')
  and sl.inv_num not in (
    select inv_num 
      from prd_adw.pos_return_header
     where trx_date between add_months(&trx_s_date,-24) and &comm_mth - 1)
  and p.inv_num not in (
        select ld_inv_num from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T) 
  and substr(p.id_num,1,8) in (
        select k.HKID_BR_PREFIX
        FROM PRD_ADW.KPI_ALL_CA_BR_LIST k
       where k.trx_month = &trx_s_date
        );
commit;



prompt 'Step B_BM_CRC_COMM_001A02_T : [ Prepare the dataset for specify trx month start from 001A01_T ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_001A02_T
(  trx_mth
   ,comm_mth
   ,case_id
   ,case_src
   ,ld_inv_num
   ,create_ts
   ,refresh_ts
   ,json_rmk
   ,ld_cust_num
   ,ld_subr_num
   ,ld_start_date
)select    trx_mth
   ,comm_mth
   ,case_id
   ,case_src
   ,ld_inv_num
   ,create_ts
   ,refresh_ts
   ,json_rmk
   ,ld_cust_num
   ,ld_subr_num
   ,ld_start_date
from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T 
where ld_start_date between &trx_s_date and &trx_e_date;
commit;


prompt 'Step B_BM_CRC_COMM_001A03_T : [ Handle the nomination flag and customer label team checking min subr_switch_on_date] ';

insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_001A03_T
(
    case_id
   ,ld_inv_num
   ,nomin_flg
   ,account_mgr
   ,team
   ,min_subr_sw_on_date
   ,create_ts
   ,refresh_ts
)
 select t.case_id
        ,t.ld_inv_num
        ,nvl(n.nomin_flg,'N') as NOMIN_FLG
        ,nvl(cl.account_mgr,' ') as team 
        ,nvl(cl.team_head,' ') as team 
        ,nvl(sw.min_subr_sw_on_date,date '2999-12-31')as min_subr_sw_on_date
        ,sysdate
        ,sysdate
  from (select * from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T where ld_start_date between &trx_s_date and &trx_e_date )t 
  left outer join prd_adw.cust_info_hist c 
      on t.ld_cust_num = c.cust_num
      and &comm_mth - 1 between c.start_date and c.end_date 
  left outer join(
      select k.HKID_BR_PREFIX
            ,k.trx_month
            ,max('Y') as NOMIN_FLG
        FROM PRD_ADW.KPI_ALL_CA_BR_LIST k
       where k.trx_month = &trx_s_date
        GROUP BY k.trx_month
                ,k.HKID_BR_PREFIX
  ) n 
    on c.hkid_br_prefix = n.hkid_br_prefix
  left outer join prd_adw.subr_ac_mgr_hist cl
    on c.hkid_br_prefix = cl.idbr_prefix
    and &comm_mth - 1 between cl.start_date and cl.end_date
  left outer join (
    Select ts.case_id
         ,nvl(nvl(min(pc.subr_sw_on_date),p.subr_sw_on_date),date '2999-12-31') as min_subr_sw_on_date           
     from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T ts
    left outer join prd_adw.subr_info_hist p
     on ts.ld_cust_num = p.cust_num
     and ts.ld_subr_num = p.subr_num
     and ts.ld_start_date between p.start_date and p.end_date
    left outer join prd_adw.subr_info_hist pc 
     on  ts.ld_cust_num <> pc.cust_num
     and ts.ld_subr_num = pc.subr_num
     and pc.subr_stat_cd ='TX'     
     and ts.ld_start_date between pc.start_date and pc.end_date
     and p.subr_sw_on_date = pc.subr_sw_off_date 
    group by  ts.case_id
         ,p.subr_sw_on_date
  )sw on t.case_id =  sw.case_id;
commit;

prompt 'Step B_BM_CRC_COMM_001A04_T : [ provide salesman code mapping ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_001A04_T
 (
     case_id
    ,nomin_flg
    ,salesman_cd
    ,acct_mgr
    ,team
    ,json_rmk
    ,create_ts
    ,refresh_ts          
 )
select t.case_id        
        ,nvl(n.nomin_flg,'N') as NOMIN_FLG
        --,nvl(cl.account_mgr,' ') as nomin_acct_mgr
        --,nvl(cl.team_head,' ') as nomin_team
        ,nvL(ph.salesman_cd,' ') as salesman_cd
        --,sbc.acct_mgr_fullname as sa_acct_mgr
        --,sbc.team_head as sa_team
        --,s.dealer_cd as dealer_cd
        --,sbd.acct_mgr_fullname as sa_acct_mgr
        --,sbd.team_head as sa_team
        ,case when ph.salesman_cd is not null and sbc.acct_mgr_fullname is not null
                then nvl(sbc.acct_mgr_fullname,' ')
             when nvl(n.nomin_flg,'N')='Y' and cl.account_mgr is not null then
                nvl(cl.account_mgr,' ')
             else ' '
         end fin_acct_mgr
        ,case when ph.salesman_cd is not null and sbc.team_head is not null
                then nvl(sbc.team_head,' ')
             when nvl(n.nomin_flg,'N')='Y' and cl.team_head is not null then
                nvl(cl.team_head,' ')
             else ' '
         end fin_team_head
        , ',"SALESINFO_SALESMAN":"'||nvl(ph.salesman_cd,'NA')||'-'||nvl(sbc.acct_mgr_fullname,'NA')||'-'||nvl(sbc.team_head,'NA')||'"'
          ||',"SALESINFO_NOMIN":"'||'NA'||'-'||nvl(cl.account_mgr,'NA')||'-'||nvl(cl.team_head,'NA')||'"' as json_rmk
        ,sysdate
        ,sysdate
  from (select * from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T 
  where ld_start_date between &trx_s_date and &trx_e_date )t
  left outer join prd_adw.subr_info_hist s
      on t.ld_subr_num = s.subr_num
     and t.ld_cust_num = s.cust_num
     and &comm_mth - 1 between s.start_date and s.end_date
  left outer join prd_adw.cust_info_hist c
      on t.ld_cust_num = c.cust_num
      and &comm_mth - 1 between c.start_date and c.end_date      
  left outer join(
      select k.HKID_BR_PREFIX
            ,k.trx_month
            ,max('Y') as NOMIN_FLG
        FROM PRD_ADW.KPI_ALL_CA_BR_LIST k
       where k.trx_month = &trx_s_date
        GROUP BY k.trx_month
                ,k.HKID_BR_PREFIX
  ) n
    on c.hkid_br_prefix = n.hkid_br_prefix
  left outer join prd_adw.pos_inv_header ph
        on t.ld_inv_num = ph.inv_num
  left outer join prd_adw.subr_ac_mgr_hist cl
    on c.hkid_br_prefix = cl.idbr_prefix
    and &comm_mth - 1 between cl.start_date and cl.end_date
  left outer join ${etlvar::ADWDB}.bm_staff_list sbc
    on ph.salesman_cd = sbc.salesman_cd
       and sbc.TRX_MTH = &trx_mth
  left outer join ${etlvar::ADWDB}.bm_staff_list sbd
    on s.dealer_cd = sbd.salesman_cd
       and sbd.TRX_MTH = &trx_mth;
commit;
   


prompt 'Step B_BM_CRC_COMM_01B01_T : [ Combine the all case filter out nomination flag not Y and team not in list fill the relate case ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T
(
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
   ,lm_subr_num
   ,lm_cust_num
   ,subr_sw_on_date
   ,subr_sw_off_date
   ,rate_plan_cd
   ,rate_plan_tariff
   ,skip_flg
   ,except_flg
   ,json_rmk)
Select 
    t.trx_mth
   ,t.comm_mth
   ,t.case_id
   ,t.case_src
   ,' ' as case_type
   ,t.ld_cust_num
   ,t.ld_subr_num
   ,t.ld_inv_num
   ,sl.ld_cd as ld_cd
   ,sl.mkt_cd as ld_mkt_cd
   ,sl.ld_start_date
   ,sl.ld_expired_date
   ,slo.ld_expired_date as ld_orig_exp_date
   ,sl.d_Ld_cd_nature_n as ld_nature
   ,ph.inv_date as ld_inv_date
   ,ph.case_id as om_order_id
   ,r2.salesman_cd
   ,r2.acct_mgr as sale_team
   ,nvl(p.dealer_cd,' ')
   ,r2.nomin_flg
   ,r2.team as cust_label_team
   ,nvl(sp.split_subr,' ') as split_subr
   ,c.cust_name
   ,r1.min_subr_sw_on_date
   ,nvl(p.subr_num,' ') lm_subr_num
   ,nvl(p.cust_num,' ') lm_cust_num
   ,nvl(p.subr_sw_on_date , date '2999-12-31') 
   ,nvl(p.subr_sw_off_date, date '2999-12-31')
   ,nvl(p.rate_plan_cd,' ')
   ,nvl(br.bill_rate,0) as rate_plan_tariff
   ,' ' as skip_flg
   ,' ' as except_flg
   ,t.json_rmk||nvl(r2.json_rmk,' ') as json_rmk
from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A02_T t
----nomin_flag ,cusomter label team , min_sw_on_date
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_001A03_T r1 
    on t.case_id = r1.case_id
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_001A04_T r2 
    on t.case_id = r2.case_id
---LD info
left outer join (
        select  t1.case_id 
                ,s.*
                ,mk.d_ld_cd_nature_n
          from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A02_T t1
              ,prd_adw.subr_ld_hist s
         left outer join prd_adw.mkt_ref_vw mk 
                on s.mkt_cd = mk.mkt_cd
        where t1.ld_inv_num = s.inv_num
        and t1.comm_mth - 1  between s.start_date and s.end_date
)sl
on t.case_id = sl.case_id
left outer join prd_adw.pos_inv_header ph
    on t.ld_inv_num = ph.inv_num
---LD orig_ld_exp_date    
left outer join (
    select t2.case_id 
          ,slor.*
      from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A02_T t2
          ,prd_adw.subr_ld_hist slor
    where t2.ld_inv_num = slor.inv_num
    and t2.ld_start_date between slor.start_date and slor.end_date         
)slo
on t.case_id= slo.case_id
left outer join (
   select t3.case_id 
         ,p.*
    from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A02_T t3
    left outer join prd_adw.subr_ld_hist sl2
        on t3.ld_inv_num = sl2.inv_num
        and t3.comm_mth -1  between sl2.start_date and sl2.end_date
    left outer join prd_adw.subr_info_hist p
        on sl2.cust_num  = p.cust_num
        and sl2.subr_num = p.subr_num
        and t3.comm_mth - 1 between p.start_date and p.end_date
)p
on t.case_id = p.case_id
left outer join (
    select t4.case_id
          ,c.*
      from  ${etlvar::TMPDB}.B_BM_CRC_COMM_001A02_T t4
    left outer join prd_adw.cust_info_hist c
    on t4.ld_cust_num  = c.cust_num    
    and t4.comm_mth - 1 between c.start_date and c.end_date
) c
on t.case_id = c.case_id
left outer join (
        Select t5.case_id
            ,sp.subr_num as split_subr
        from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A02_T t5
             ,prd_adw.subr_info_hist sp
        where 
            t5.ld_subr_num = '448'||sp.subr_num       
        and t5.ld_start_date between sp.start_date and sp.end_date
        and t5.ld_cust_num = sp.cust_num
        and sp.subr_stat_cd ='OK'
) sp
on t.case_id = sp.case_id
left outer join prd_adw.bill_serv_ref br
    on p.rate_plan_cd = br.bill_serv_cd 
  and t.comm_mth -1 between br.eff_start_date and br.eff_end_date 
left outer join prd_adw.mkt_ref_vw m
    on sl.mkt_cd = m.mkt_cd
where r1.nomin_flg = 'Y' 
  and r1.team not in ('BM PR TEAM','CORP TEAM')
  and t.ld_start_date between &trx_s_date and &trx_e_date;
commit;

-----Exceptional checking bypass furture csae ---   
update ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T t
   set  t.skip_flg='Y'
        ,t.except_flg=t.except_flg||'EXCEPT_SKIP_3MTH_CONTRACT'
where t.case_id in (
        Select t.case_id 
        from ${etlvar::TMPDB}.B_BM_CRC_COMM_001A02_T t
            ,${etlvar::TMPDB}.B_BM_CRC_COMM_001A01_T t2
        where t.ld_subr_num = t2.ld_subr_num 
        and t.ld_start_date between &trx_s_date and &trx_e_date
        and t2.ld_start_date > &trx_e_date
);
commit;


prompt 'Step B_BM_CRC_COMM_001B01_T : [ Update which is new activation case with min_subr_sw_on_date within trx mth] ';
update ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T t
   set  t.skip_flg='Y'
        ,t.except_flg=t.except_flg||'EXCEPT_NEW_ACTV_CASES'
where  t.min_subr_sw_on_date between &trx_s_date and &trx_e_date ;
commit;

update ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T t
   set  t.skip_flg='Y'
        ,t.except_flg=t.except_flg||'EXCEPT_NEW_ACTV_COMM_PAID'
where  t.case_id in (
 select t.case_id 
 from 
    ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T t
   ,${etlvar::ADWDB}.BM_CNC_COMM_H n
 where t.ld_inv_num = n.ld_inv_num
 and n.comm_mth between add_months(t.comm_mth ,-4 ) and t.comm_mth
);
commit;


prompt 'Step B_BM_CRC_COMM_001B01_T : [ Mark Enterphase Soultion flag CASE_ES and SKIP it ] ';
update ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T t
    set  t.case_type = 'CASE_ES'
        ,t.skip_flg='Y'         
        ,t.except_flg=t.except_flg||',EXCEPT_SKIP_CASE_ES'
   where t.case_id in (
        select  t.case_id 
          from  ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T t
                ,prd_adw.prepd_postpaid_subr_n1 n1
        where cproj_flg='Y'
          and t.ld_subr_num = n1.subr_num
          and t.ld_cust_num = n1.cust_num
);
commit;
prompt 'Step B_BM_CRC_COMM_001B01_T : [ Mark those cases have no lm profile ld ] ';
update ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T t
  set skip_flg='Y'
      ,except_flg =except_flg||',EXCEPT_SKIP_NO_LM_PROFILE'
where nvl(t.lm_subr_num,' ') = ' ';
commit;


prompt 'Step B_BM_CRC_COMM_001B01_T : [ Mark duplicate cases and use longset ld expired_date one others SKIP it ] ';
update ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T 
  set skip_flg='Y'
      ,except_flg =except_flg||',EXCEPT_SKIP_DUPLICATE_CASE'
where case_id not in (
       Select max(t.case_id) keep( dense_rank first order by ld_expired_date desc
                ,decode(ld_nature,'HANDSET LD',1,'SIM only LD',2,'No contract',3,'F',4,'P',5,'Others',6,'VAS LD',7,8)) skip_case_id
       from ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T t
       where t.skip_flg<>'Y'       
       group by
             t.ld_subr_num
             ,t.ld_cust_num)
      and skip_flg<>'Y';
commit;
 
prompt 'Step B_BM_CRC_COMM_001_T : [ To result table  ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T
(
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
   ,contract_mth
   ,om_order_id
   ,saleman_cd
   ,sale_team
   ,dealer_cd
   ,normin_flg
   ,cust_label_team
   ,split_subr
   ,cust_name
   ,min_subr_sw_on_date
   ,lm_subr_num
   ,lm_cust_num
   ,subr_sw_on_date
   ,subr_sw_off_date
   ,rate_plan_cd
   ,rate_plan_tariff
   ,skip_flg
   ,except_flg
   ,idbr_prefix
   ,json_rmk
)select
    t.trx_mth
   ,t.comm_mth
   ,t.case_id
   ,t.case_src
   ,t.case_type
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
   ,nvl(substr(t.ld_cd,4,2),0) as contract_mth
   ,t.om_order_id
   ,t.saleman_cd
   ,t.sale_team
   ,t.dealer_cd
   ,t.normin_flg
   ,t.cust_label_team
   ,t.split_subr
   ,t.cust_name
   ,t.min_subr_sw_on_date
   ,t.lm_subr_num
   ,t.lm_cust_num
   ,t.subr_sw_on_date
   ,t.subr_sw_off_date
   ,t.rate_plan_cd
   ,t.rate_plan_tariff
   ,t.skip_flg
   ,t.except_flg
   ,nvl(c.hkid_br_prefix,' ')
   ,t.json_rmk
from ${etlvar::TMPDB}.B_BM_CRC_COMM_001B01_T t
left outer join prd_adw.cust_info_hist c
on t.ld_cust_num =c.cust_num
and &comm_mth - 1 between c.start_date and c.end_date;
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

