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

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_001A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_001A02_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_001A03_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CNC_COMM_001_T');

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
prompt 'Step B_BM_CNC_COMM_001A01_T : [Prepare base of all new activation case ] ';

insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_001A01_T(
     trx_mth
    ,comm_mth
    ,case_id
    ,case_src
    ,case_type
    ,cust_num
    ,subr_num
    ,acct_num
    ,dealer_cd
    ,min_subr_sw_on_date
    ,subr_sw_on_date
    ,subr_sw_off_date
    ,orig_rate_plan_cd
    ,rate_plan_cd
) 
select 
     &trx_mth
    ,&comm_mth
    ,'CNC'||to_char(&trx_mth,'yymm')||'-'||s.subr_num||'-'||s.cust_num as case_id
    ,'SRC_PROFILE' as case_src 
    ,' ' case_type
    ,s.cust_num
    ,s.subr_num
    ,s.acct_num
    ,s.dealer_cd
    ,s.subr_sw_on_date as  min_subr_sw_on_date
    ,s.subr_sw_on_date
    ,nvl(sp.subr_sw_off_date,date '2999-12-31') as subr_sw_off_date
    ,s.rate_plan_cd  orig_rate_plan_cd
    ,sp.rate_plan_cd rate_plan_cd
from prd_adw.subr_info_hist s
left outer join ${etlvar::ADWDB}.bm_staff_list st
        on s.dealer_cd = st.salesman_cd
           and st.TRX_MTH = &trx_mth
left outer join prd_adw.subr_info_hist sp
    on s.subr_num = sp.subr_num
   and s.cust_num = sp.cust_num
   and &comm_mth - 1 between sp.start_date and sp.end_date
 where 
---replace to refer bm staff list
   st.salesman_cd is not null
  --s.dealer_cd like 'CA%'
  and s.subr_sw_on_date between &trx_s_date and &trx_e_date
  and &trx_e_date between s.start_date and s.end_date
  and s.subr_stat_cd ='OK';
commit;




prompt 'Step B_BM_CRC_COMM_001A01_T : [ map with ld ] ';
insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_001A02_T
 (
    trx_mth
    ,comm_mth
    ,case_id
    ,ld_inv_num
 )
   select t.trx_mth
         ,t.comm_mth
         ,t.case_id
         ,max(sl.inv_num) keep (dense_rank first order by 
                    case when substr(sl.ld_cd,5,1)='M' then to_number(substr(sl.ld_cd,4,2)) * to_number(substr(sl.ld_cd,7))
                        else to_number(substr(sl.ld_cd,7)) end  desc
                    ,sl.ld_start_date desc
                    ,substr(sl.ld_cd,4,2) desc)                                          
    from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A01_T t
  left outer join prd_adw.subr_ld_hist sl
     on &comm_mth - 1 >= sl.start_date 
    --and sl.end_date >= &trx_e_date 
    and &comm_mth - 1 between sl.ld_start_date and sl.ld_expired_date
    and sl.ld_start_date between &trx_s_date and &comm_mth - 1
    and t.cust_num = sl.cust_num
    and t.subr_num = sl.subr_num
  where sl.mkt_cd not in (select m.mkt_cd from prd_adw.mkt_ref_vw m where m.ld_cd_nature in ('V') )
    and sl.void_flg<>'Y'
    and sl.waived_flg <>'Y'
    and sl.billed_flg <>'Y'
    and sl.mkt_cd <>'RENEW'
  group by t.trx_mth
         ,t.comm_mth
         ,t.case_id;
commit;
prompt 'Step B_BM_CRC_COMM_001A03_T : [ combine all result table ] ';

insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T
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
        ,ld_contract_period
        ,dealer_cd
        ,split_acct
        ,split_subr
        ,skip_flg
        ,except_flg
        ,json_rmk
        ,cust_id_type
)
select 
         t.trx_mth
        ,t.comm_mth
        ,t.case_id
        ,t.case_src
        ,' ' case_type
        ,t.cust_num
        ,t.subr_num
        ,t.acct_num
        ,t.subr_sw_on_date
        ,t.subr_sw_off_date
        ,nvl(mi.min_subr_sw_on_date,t.subr_sw_on_date)  as min_subr_sw_on_date
        ,t.rate_plan_cd
        ,t.orig_rate_plan_cd
        ,nvl(sl.cust_num,' ') as ld_cust_num 
        ,nvl(sl.subr_num,' ') as ld_subr_num
        ,nvl(sl.inv_num,' ') as ld_inv_num
        ,nvl(sl.ld_cd,' ') as ld_cd
        ,nvl(sl.mkt_cd,' ') as ld_mkt_cd
        ,nvl(sl.ld_start_date,date '2999-12-31')  as ld_start_date
        ,nvl(sl.ld_expired_date,date '2999-12-31') as ld_expired_date
        ,nvl(sl_orig.ld_orig_exp_date,date '2999-12-31') as ld_orig_exp_date
        ,nvl(mk.ld_cd_nature,' ') as  ld_nature
        ,nvl(ph.inv_date,date '2999-12-31') as ld_inv_date
        ,case when sl.ld_cd is not null then to_number(substr(sl.ld_cd,4,2)) else 0 end
        ,t.dealer_cd
        ,nvl(spt.acct_num,' ') as split_acct
        ,nvl(spt.subr_num,' ') as split_subr
        ,' ' as skip_flg
        ,' ' as except_flg
        ,',"ID_TYPE_CD":"'||idt.id_type_cd||'"' 
         ||',"ORIG_RATE_PLAN_CD":"'||t.orig_rate_plan_cd||'"' as json_rmk
        ,case when idt.id_type_cd in ('B','G','N') then 'CUSTID_BR' 
              else 'CUSTID_HKID'
         end cust_id_type
from( 
        select   tt.*
                ,nvl(l.ld_inv_num,' ')as ld_inv_num
        from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A01_T tt
        left outer join  ${etlvar::TMPDB}.B_BM_CNC_COMM_001A02_T l
        on tt.case_id = l.case_id
)t
left outer join prd_adw.subr_ld_hist sl
        on &comm_mth - 1 between sl.start_date and sl.end_date
     and t.ld_inv_num = sl.inv_num
left outer join prd_adw.pos_inv_header ph
        on t.ld_inv_num = ph.inv_num
left outer join prd_adw.mkt_ref_vw mk
        on sl.mkt_cd = mk.mkt_cd
---- ld_orig_exp_date
left outer join (
        select sl2.case_id 
              ,min(l.ld_expired_date)keep(dense_rank first order by l.ld_start_date asc) ld_orig_exp_date
        from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A02_T sl2
            ,prd_adw.subr_ld_hist l
        where sl2.ld_inv_num = l.inv_num
                group by sl2.case_id
)sl_orig
   on t.case_id = sl_orig.case_id
left outer join (
  Select t.case_id
          ,p.subr_num
          ,p.acct_num
    from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A01_T t
        ,prd_adw.subr_info_hist p
    where   &comm_mth -1 between p.start_date and p.end_date
      and p.subr_stat_cd in ('OK','SU')    
      and t.subr_num ='448'||p.subr_num
      and t.cust_num = p.cust_num 
) spt
 on t.case_id = spt.case_id
left outer join (
 Select t.case_id
        ,max(a.id_type_cd)keep(dense_rank first order by decode(a.d_cust_stat,'Active',0,1)) id_type_cd         
  from PRD_BIZ_SUMM.CUST_MISC_INFO a
      ,${etlvar::TMPDB}.B_BM_CNC_COMM_001A01_T t
  where t.cust_num = a.cust_num
  group by t.case_id
)idt
 on t.case_id = idt.case_id 
left outer join (
        Select t.case_id
      ,min(s.subr_sw_on_date) min_subr_sw_on_date
  from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A01_T t
      ,prd_adw.subr_info_hist s
  where t.subr_num = s.subr_num    
    and t.subr_sw_on_date between s.start_date and s.end_date
    and s.subr_stat_cd = 'TX'  
    and s.subr_sw_off_date = t.subr_sw_on_date
    and (t.cust_num <> s.cust_num or t.acct_num <> s.acct_num)
  group by t.case_id
)mi
  on t.case_id = mi.case_id ;
commit;

prompt 'Step B_BM_CRC_COMM_001A03_T : [ update the case type]';
update ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T tt
 set tt.case_type ='CASE_ES'
where tt.case_id in(
        select  t.case_id
          from  ${etlvar::TMPDB}.B_BM_CNC_COMM_001A01_T t
                ,prd_adw.prepd_postpaid_subr_n1 n1
        where cproj_flg='Y'
          and t.subr_num = n1.subr_num
          and t.cust_num = n1.cust_num
);
commit;

----- Map case_BR
update ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T tt
set tt.case_type ='CASE_BR'
where tt.rate_plan_cd in (
    SELECT
           r.BILL_SERV_CD
     FROM PRD_ADW.BM_COMM_BILL_CD_REF r
     where r.trx_month = &trx_s_date
     and r.subcategory='H4'
);
commit;

----- Map case_baisc telecom
update ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T tt
set tt.case_type ='CASE_BT'
where tt.case_type =' ' ;
commit;


prompt 'Step B_BM_CRC_COMM_001A03_T : [ update the case which is not specify subr_sw_on_date ] ';
--update ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
--   set t.skip_flg='Y'
--      ,except_flg ='EXCEPT_SKIP_NO_NEWACTV'
-- where t.min_subr_sw_on_date not between  &trx_s_date and &trx_e_date;
--commit;

update ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
   set t.skip_flg='Y'
      ,except_flg ='EXCEPT_SKIP_NO_LDMAP'
 where t.ld_inv_num =' ';
commit;

update ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
   set t.skip_flg='Y'
      ,except_flg ='EXCEPT_SKIP_CASE_ES'
 where t.case_type ='CASE_ES ';
commit;

update ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
   set t.skip_flg='Y'
      ,except_flg =except_flg ||';'||'EXCEPT_SKIP_NO_NEWACTV'
 where t.min_subr_sw_on_date not between &trx_s_date and &trx_e_date;
commit;

update ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
   set t.skip_flg='Y'
      ,except_flg =except_flg ||';'||'EXCEPT_SKIP_CHG_CUST'
 where t.case_id in (
        Select t.case_id
          from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
              ,prd_adw.subr_info_hist st
        where t.ld_subr_num = st.subr_num
          and t.ld_cust_num <> st.cust_num
          and t.subr_sw_on_date between &trx_s_date and &trx_e_date
          and st.subr_sw_off_date = t.subr_sw_on_date 
          and st.subr_stat_cd ='TX'
          and &trx_e_date between st.start_date and st.end_date
);
commit;



prompt 'Step B_BM_CRC_COMM_001_T : [ insert into target table of 01 ] ';
insert into ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T
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
        ,ld_contract_period
        ,dealer_cd
        ,split_acct
        ,split_subr
        ,skip_flg
        ,except_flg
        ,json_rmk
        ,cust_id_type
        ,acct_mgr
        ,team_head
        ,comm_type2
        ,comm_type3
        ,plan_tariff
        ,cust_name
        ,idbr_prefix
        ,lm_cust_num
        ,lm_subr_num
)
select 
         t.trx_mth
        ,t.comm_mth
        ,t.case_id
        ,t.case_src
        ,t.case_type
        ,t.cust_num
        ,t.subr_num
        ,t.acct_num
        ,t.subr_sw_on_date
        ,t.subr_sw_off_date
        ,t.min_subr_sw_on_date
        ,t.rate_plan_cd
        ,t.orig_rate_plan_cd
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
        ,t.ld_contract_period
        ,t.dealer_cd
        ,t.split_acct
        ,t.split_subr
        ,t.skip_flg
        ,t.except_flg
        ,t.json_rmk
                ||',"SALES_DEALER":"'||t.dealer_cd||'-'||nvl(s.acct_mgr, 'NA')||'-'||nvl(s.team_head,'NA')||'"'
                ||',"SALES_NOMINATION":"'||'NA'||'-'||nvl(brs.acct_mgr, 'NA')||'-'||nvl(brs.team_head,'NA')||'"'
        ,t.cust_id_type
        ,nvl(s.acct_mgr,nvl(brs.acct_mgr,' ')) 
        ,nvl(s.team_head,nvl(brs.team_head,' '))
        ,nvl(br.comm_type2,' ' )
        ,nvl(br.comm_type3,' ' )
        ,nvl(br.plan_tariff,0 )
        ,nvl(cu.cust_name,' ' )
        ,nvl(cu.idbr_prefix,' ' )
        ,nvl(lsl.lm_cust_num,' ')
        ,nvl(lsl.lm_subr_num,' ')
from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
left outer join (
        Select t.case_id,max(st.acct_mgr_fullname) acct_mgr, max(st.team_head) team_head
        from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
            ,${etlvar::ADWDB}.bm_staff_list st
        where t.dealer_cd = st.salesman_cd
              and st.TRX_MTH = &trx_mth
        group by t.case_id
--Select t.case_id,max(a.account_mgr) acct_mgr, max(a.team_head) team_head
--  from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
--      ,prd_adw.cust_info_hist c
--      ,prd_adw.SUBR_AC_MGR_HIST a
-- where t.cust_num = c.cust_num
--   and &comm_mth - 1 between c.start_date and c.end_date
--   and c.hkid_br_prefix = a.idbr_prefix
--   and &comm_mth - 1 between a.start_date and a.end_date
--  group by t.case_id
)s on t.case_id = s.case_id
left outer join (
Select tr.case_id
        ,b.type2 as comm_type2
        ,b.type3 as comm_type3
        ,r.bill_rate as plan_tariff
  from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T tr
  left outer join ${etlvar::ADWDB}.BM_COMM_RPT_ICT_REF b
    on tr.rate_plan_cd = b.BILL_CD
  left outer join prd_adw.bill_serv_ref r
    on tr.rate_plan_cd = r.bill_serv_cd
    and &trx_e_date between r.eff_start_date and r.eff_end_date
) br on t.case_id = br.case_id 
left outer join (
Select t.case_id,max(c.cust_name) cust_name,max(c.hkid_br_prefix) idbr_prefix
  from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
      ,prd_adw.cust_info_hist c
 where t.cust_num = c.cust_num
   and &trx_e_date between c.start_date and c.end_date
 group by t.case_id
) cu 
on t.case_id  = cu.case_id
left outer join (
     Select tt.case_id
        ,s.cust_num  lm_cust_num 
        ,s.subr_num  lm_subr_num 
     from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T tt
         ,prd_adw.subr_ld_hist s
     where tt.ld_inv_num = s.inv_num
       and &comm_mth - 1 between s.start_date and s.end_date
) lsl
on t.case_id =lsl.case_id
left outer join (
 Select t.case_id,max(a.account_mgr) acct_mgr, max(a.team_head) team_head
  from ${etlvar::TMPDB}.B_BM_CNC_COMM_001A03_T t
      ,prd_adw.cust_info_hist c
      ,prd_adw.SUBR_AC_MGR_HIST a
 where t.cust_num = c.cust_num
   and &comm_mth - 1 between c.start_date and c.end_date
   and c.hkid_br_prefix = a.idbr_prefix
   and &comm_mth - 1 between a.start_date and a.end_date
  group by t.case_id
) brs
on t.case_id = brs.case_id
;
commit;



update ${etlvar::TMPDB}.B_BM_CNC_COMM_001_T t
   set t.skip_flg='Y'
      ,except_flg ='EXCEPT_NO_ACC_MGR'
 where t.acct_mgr=' ';
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
