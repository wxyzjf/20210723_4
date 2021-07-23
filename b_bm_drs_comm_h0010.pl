/opt/etl/prd/etl/APP/ADW/B_BM_DRS_COMM_H/bin> cat b_bm_drs_comm_h0010.pl 
#####################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29 
#
#
######################################################

my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;

my $MASTER_TABLE = ""; #Please input the final target ADW table name here
my $TMPDB="${etlvar::TMPDB}";
my $ADWDB="${etlvar::ADWDB}";


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


set define on;
define tx_date = trunc(to_date('$etlvar::TXDATE','YYYY-MM-DD'),'MM');
define comm_mth = add_months(&tx_date,-1);
define trx_mth = add_months(&comm_mth,-4);
define trx_s_date = add_months(&comm_mth,-4);
define trx_e_date = add_months(&comm_mth,-3)-1;

EXECUTE ${etlvar::UTLDB}.ETL_UTILITY.truncate_tbl2(P_schema_tbl_name=>'${TMPDB}.B_bm_drs_COMM_001_T');
EXECUTE ${etlvar::UTLDB}.ETL_UTILITY.truncate_tbl2(P_schema_tbl_name=>'${TMPDB}.B_bm_drs_COMM_002_T');
EXECUTE ${etlvar::UTLDB}.ETL_UTILITY.truncate_tbl2(P_schema_tbl_name=>'${TMPDB}.B_bm_drs_COMM_003_T');

---- New actvation base ----
insert into ${TMPDB}.B_bm_drs_COMM_001_T
 (  TRX_MTH 
    ,COMM_MTH
    ,CASE_ID
    ,CASE_SRC
    ,CUST_NUM
    ,SUBR_NUM
    ,ACCT_NUM
    ,DEALER_CD
    ,SALESMAN_CD
    ,SUBR_SW_ON_DATE
    ,SUBR_SW_OFF_DATE
    ,rate_plan_cd
    ,CUST_NAME
    ,HKID_BR_PREFIX
    ,ACCT_MGR
    ,TEAM_HEAD
    ,BILL_SERV_CD
    ,BILL_START_DATE
    ,BILL_END_DATE
    ,TYPE1
    ,TYPE2
    ,TYPE3
    ,BILL_RATE
    ,EXCEPT_RMK
    ,CREATE_TS
    ,REFRESH_TS)
select
     &trx_mth as trx_mth
    ,&comm_mth as comm_mth
    ,to_char(&trx_mth,'yymm')||'-'||bs.subr_num||'-'||bs.bill_serv_cd as case_id
    ,'SRC_PROFILE' as case_src    
    ,bs.cust_num
    ,bs.subr_num
    ,nvl(s.acct_num,' ') as acct_num
    ,bs.salesman_cd
    ,BS.SALESMAN_CD
    ,nvl(s.subr_sw_on_date,date '2999-12-31')
    ,nvl(s.subr_sw_off_date,date '2999-12-31')
    ,nvl(s.rate_plan_cd,' ') rate_plan_cd
    ,nvl(c.CUST_NAME,' ') as cust_name
    ,nvl(c.HKID_BR_PREFIX,' ') as hkid_br_prefix
    ,nvl(nvl(st2.acct_mgr_fullname,st.acct_mgr_fullname),' ') as acct_mgr
    ,nvl(nvl(st2.team_head,st.team_head),' ') team_head
    ,bs.bill_serv_cd
    ,bs.bill_start_date
    ,bs.bill_end_date
    ,nvl(t_v6.type1,' ')
    ,nvl(t_v6.type2,' ')
    ,nvl(t_v6.type3,' ')
    ,nvl(br.bill_rate,0)
    ,CASE WHEN s.subr_num is null then 'EXCEPT_NOFOUND_PROFILE;' else ' ' end except_rmk 
    ,sysdate
    ,sysdate
from ${ADWDB}.bill_servs bs
left outer join (select * from ${ADWDB}.BM_COMM_RPT_ICT_REF where upper(type2) in ('CALL GUARD','DRS'))t_v6
     on bs.bill_serv_cd = t_v6.bill_cd
left outer join ${ADWDB}.subr_info_hist s 
     on bs.subr_num = s.subr_num
    and bs.cust_num = s.cust_num
    and &trx_e_date  between s.start_date and s.end_date
left outer join ${ADWDB}.cust_info_hist c 
     on bs.cust_num = c.cust_num
    and &trx_e_date  between c.start_date and c.end_date
left outer join ${etlvar::ADWDB}.bm_staff_list st
        on bs.dealer_cd = st.salesman_cd
           and st.TRX_MTH = &trx_mth
left outer join ${etlvar::ADWDB}.bm_staff_list st2
        on bs.salesman_cd = st2.salesman_cd
           and st2.TRX_MTH = &trx_mth
left outer join ${ADWDB}.bill_serv_ref br
        on bs.bill_serv_cd = br.bill_serv_cd
       and &trx_e_date between br.eff_start_date and br.eff_end_date                       
where &trx_e_date between bs.bill_start_date and bs.bill_end_date
 and bs.bill_start_date between &trx_s_date and &trx_e_date
 and t_v6.bill_cd is not null;


commit;


---- prepare DRS ld  ----
insert into ${TMPDB}.B_bm_drs_COMM_002_T(
trx_mth
,comm_mth
,case_id
,case_src
,cust_num
,subr_num
,acct_num
,dealer_cd
,salesman_cd
,subr_sw_on_date
,subr_sw_off_date
,rate_plan_cd
,cust_name
,hkid_br_prefix
,acct_mgr
,team_head
,bill_serv_cd
,bill_start_date
,bill_end_date
,type1
,type2
,type3
,bill_rate
,ld_inv_num
,ld_cd
,ld_mkt_cd
,ld_start_date
,ld_expired_date
,ld_contract_mth
,last_bill_serv_cd
,last_bill_start_date
,last_bill_end_date
,final_bill_serv_cd
,final_bill_start_date
,final_bill_end_date
,except_rmk
,create_ts
,refresh_ts)
    Select
       t.TRX_MTH 
        ,t.COMM_MTH
        ,t.CASE_ID
        ,t.CASE_SRC
        ,t.CUST_NUM
        ,t.SUBR_NUM
        ,t.ACCT_NUM
        ,t.DEALER_CD
        ,T.SALESMAN_CD
        ,t.SUBR_SW_ON_DATE
        ,t.SUBR_SW_OFF_DATE
        ,t.rate_plan_cd
        ,t.cust_name
        ,t.hkid_br_prefix
        ,t.ACCT_MGR
        ,t.TEAM_HEAD
        ,t.BILL_SERV_CD
        ,t.BILL_START_DATE
        ,t.BILL_END_DATE
        ,t.TYPE1
        ,t.TYPE2
        ,t.TYPE3
        ,t.BILL_RATE     
        ,nvl(max(sl.inv_num) keep(dense_rank first order by decode(t.bill_rate,to_number(substr(sl.ld_cd,7)),1,99),decode(mv.ld_cd_nature,'V',1,99),to_number(substr(sl.ld_cd,7)),sl.ld_expired_date desc),' ') ld_inv_num
        ,nvl(max(sl.ld_cd) keep(dense_rank first order by decode(t.bill_rate,to_number(substr(sl.ld_cd,7)),1,99),decode(mv.ld_cd_nature,'V',1,99),to_number(substr(sl.ld_cd,7)),sl.ld_expired_date desc),' ') ld_cd
        ,nvl(max(sl.mkt_cd) keep(dense_rank first order by decode(t.bill_rate,to_number(substr(sl.ld_cd,7)),1,99),decode(mv.ld_cd_nature,'V',1,99),to_number(substr(sl.ld_cd,7)),sl.ld_expired_date desc),' ') ld_mkt_cd
        ,nvl(max(sl.ld_start_date) keep(dense_rank first order by decode(t.bill_rate,to_number(substr(sl.ld_cd,7)),1,99),decode(mv.ld_cd_nature,'V',1,99),to_number(substr(sl.ld_cd,7)),sl.ld_expired_date desc),to_date('29991231','yyyymmdd'))ld_start_date
        ,nvl(max(sl.ld_expired_date) keep(dense_rank first order by decode(t.bill_rate,to_number(substr(sl.ld_cd,7)),1,99),decode(mv.ld_cd_nature,'V',1,99),to_number(substr(sl.ld_cd,7)),sl.ld_expired_date desc),to_date('29991231','yyyymmdd')) ld_expired_date
        ,nvl(max(substr(sl.ld_cd,4,2)) keep(dense_rank first order by decode(t.bill_rate,to_number(substr(sl.ld_cd,7)),1,99),decode(mv.ld_cd_nature,'V',1,99),to_number(substr(sl.ld_cd,7)),sl.ld_expired_date desc),'1')  ld_contract_mth
        ,nvl(max(tlm.bill_serv_cd),' ')  as last_bill_serv_cd
        ,nvl(max(tlm.bill_start_date),to_date('29991231','yyyymmdd')) as  last_bill_start_date
        ,nvl(max(tlm.bill_end_date),to_date('29991231','yyyymmdd')) as  last_bill_end_date
        ,nvl(max(lm.bill_serv_cd),' ')  as final_bill_serv_cd
        ,nvl(max(lm.bill_start_date),to_date('29991231','yyyymmdd')) as  final_bill_start_date
        ,nvl(max(lm.bill_end_date),to_date('29991231','yyyymmdd')) as  final_bill_end_date
        ,t.except_rmk
        ,sysdate
        ,sysdate
     from ${TMPDB}.B_bm_drs_COMM_001_T t
     left outer join ${ADWDB}.subr_ld_hist sl
        on t.subr_num = sl.subr_num
        and t.cust_num =sl.cust_num
        and &trx_e_date between sl.start_date and sl.end_date
        and &trx_e_date between sl.ld_start_date and sl.ld_expired_date
        and sl.ld_start_date between &trx_s_date and &trx_e_date
        and sl.ld_cd like 'LDJ%M%'
        and sl.VOID_FLG <> 'Y' and sl.WAIVED_FLG <> 'Y' and sl.BILLED_FLG <> 'Y'
     left outer join ${ADWDB}.mkt_ref_vw mv
        on sl.mkt_cd = mv.mkt_cd
     left outer join ( 
        ----- Catpure the trx_s_date - 1 snapshot (last month) same type bill code
        Select t1.case_id
        ,max(bsr.bill_serv_cd) keep (dense_rank first order by bsr.bill_start_date desc) bill_serv_cd
        ,max(bsr.bill_start_date) keep (dense_rank first order by bsr.bill_start_date desc) bill_start_date
        ,max(bsr.bill_end_date) keep (dense_rank first order by bsr.bill_start_date desc) bill_end_date
        from ${TMPDB}.B_bm_drs_comm_001_T t1
          ,${ADWDB}.bill_servs bsr
          ,(select * from ${ADWDB}.BM_COMM_RPT_ICT_REF where upper(type2) in ('CALL GUARD','DRS'))t_v6
         where t1.subr_num = bsr.subr_num
         and &trx_s_date - 1 between bsr.bill_Start_date and bsr.bill_end_date
         and bsr.bill_serv_cd = t_v6.bill_cd     
         and t1.type1 = t_v6.type1
         and t1.type2 = t_v6.type2
         and t1.type3 = t_v6.type3
        group by t1.case_id
     )tlm
        on t.case_id = tlm.case_id
    left outer join (
        Select t1.case_id
        ,max(bsr.bill_serv_cd) keep (dense_rank first order by bsr.bill_start_date desc) bill_serv_cd
        ,max(bsr.bill_start_date) keep (dense_rank first order by bsr.bill_start_date desc) bill_start_date
        ,max(bsr.bill_end_date) keep (dense_rank first order by bsr.bill_start_date desc) bill_end_date
        from ${TMPDB}.B_bm_drs_comm_001_T t1
      ,${ADWDB}.bill_servs bsr
      ,(select * from ${ADWDB}.BM_COMM_RPT_ICT_REF where upper(type2) in ('CALL GUARD','DRS'))t_v6
    where t1.subr_num = bsr.subr_num
     and &comm_mth -1 between bsr.bill_Start_date and bsr.bill_end_date
     and bsr.bill_serv_cd = t_v6.bill_cd     
     and t1.type1 = t_v6.type1
     and t1.type2 = t_v6.type2
     and t1.type3 = t_v6.type3
        group by t1.case_id  
    )   lm    
    on t.case_id = lm.case_id        
    group by t.TRX_MTH 
        ,t.COMM_MTH
        ,t.CASE_ID
        ,t.CASE_SRC
        ,t.CUST_NUM
        ,t.SUBR_NUM
        ,t.ACCT_NUM
        ,t.DEALER_CD
        ,t.SUBR_SW_ON_DATE
        ,t.SUBR_SW_OFF_DATE
        ,t.rate_plan_cd
        ,t.cust_name
        ,t.hkid_br_prefix
        ,t.ACCT_MGR
        ,t.TEAM_HEAD
        ,t.BILL_SERV_CD
        ,t.BILL_START_DATE
        ,t.BILL_END_DATE
        ,t.TYPE1
        ,t.TYPE2
        ,t.TYPE3
        ,t.BILL_RATE   
        ,t.except_rmk
        ,T.SALESMAN_CD;
commit;


insert into ${TMPDB}.B_bm_drs_COMM_003_T
(
trx_mth
,comm_mth
,case_id
,case_src
,cust_num
,subr_num
,acct_num
,dealer_cd
,salesman_cd
,subr_sw_on_date
,subr_sw_off_date
,rate_plan_cd
,cust_name
,hkid_br_prefix
,acct_mgr
,team_head
,bill_serv_cd
,bill_start_date
,bill_end_date
,type1
,type2
,type3
,bill_rate
,ld_inv_num
,ld_cd
,ld_mkt_cd
,ld_start_date
,ld_expired_date
,ld_contract_mth
,last_bill_serv_cd
,last_bill_start_date
,last_bill_end_date
,final_bill_serv_cd
,final_bill_start_date
,final_bill_end_date
,except_rmk
,create_ts
,refresh_ts
,bill_salesman_cd
,ld_salesman_cd
,json_rmk
)
select 
     bs.trx_mth
    ,bs.comm_mth
    ,bs.case_id
    ,bs.case_src
    ,bs.cust_num
    ,bs.subr_num
    ,bs.acct_num
    ,bs.dealer_cd
    ,bs.salesman_cd
    ,bs.subr_sw_on_date
    ,bs.subr_sw_off_date
    ,bs.rate_plan_cd
    ,bs.cust_name
    ,bs.hkid_br_prefix
    ,nvl(nvl(nvl(st1.ACCT_MGR_FULLNAME,st2.ACCT_MGR_FULLNAME),samh.ACCOUNT_MGR),' ') as acct_mgr
    ,nvl(nvl(nvl(st1.TEAM_HEAD,st2.TEAM_HEAD),samh.TEAM_HEAD),' ') as team_head
    ,bs.bill_serv_cd
    ,bs.bill_start_date
    ,bs.bill_end_date
    ,bs.type1
    ,bs.type2
    ,bs.type3
    ,bs.bill_rate
    ,bs.ld_inv_num
    ,bs.ld_cd
    ,bs.ld_mkt_cd
    ,bs.ld_start_date
    ,bs.ld_expired_date
    ,bs.ld_contract_mth
    ,bs.last_bill_serv_cd
    ,bs.last_bill_start_date
    ,bs.last_bill_end_date
    ,bs.final_bill_serv_cd
    ,bs.final_bill_start_date
    ,bs.final_bill_end_date
    ,case when st1.ACCT_MGR_FULLNAME is null
               and st2.ACCT_MGR_FULLNAME is null
               and samh.ACCOUNT_MGR is null
               then bs.EXCEPT_RMK || ';' || 'SKIP_UNMAP_SALESMAN'
          else bs.EXCEPT_RMK 
     end as EXCEPT_RMK
    ,sysdate
    ,sysdate
    ,bs.salesman_cd as bill_salesman_cd
    ,pih.SALESMAN_CD as ld_salesman_cd
    ,'"sales_info_bill":"' || bs.salesman_cd || '-' || st1.ACCT_MGR_FULLNAME || '-' || st1.TEAM_HEAD 
    || '","sales_info_ld":"' || bs.salesman_cd || '-' || st2.ACCT_MGR_FULLNAME || '-' || st2.TEAM_HEAD 
    || '","sales_info_nominaton":"' ||bs.salesman_cd || '-' || samh.ACCOUNT_MGR || '-' || samh.TEAM_HEAD 
    || '"' as json_rmk
from ${TMPDB}.B_bm_drs_COMM_002_T bs
left outer join ${etlvar::ADWDB}.bm_staff_list st1
    on bs.salesman_cd = st1.salesman_cd
       and st1.TRX_MTH = &trx_mth
left outer join ${ADWDB}.pos_inv_header pih
    on bs.LD_INV_NUM = pih.INV_NUM
left outer join ${etlvar::ADWDB}.bm_staff_list st2
    on pih.salesman_cd = st2.salesman_cd
       and st2.TRX_MTH = &trx_mth
left outer join ${ADWDB}.subr_ac_mgr_hist samh
    on bs.HKID_BR_PREFIX = samh.IDBR_PREFIX
       and &trx_e_date  between samh.start_date and samh.end_date;


commit;

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














