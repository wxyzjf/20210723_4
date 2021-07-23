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

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_004_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_004A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_004B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_004B02_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_BM_CRC_COMM_004C01_T');
---- comm paid at 2021-01
---- last mth of comm paid snapshot at 2020-12-31

set define on;
define tx_date = trunc(to_date('$etlvar::TXDATE','YYYY-MM-DD'),'MM');
define comm_mth = add_months(&tx_date,-1);
define trx_mth = add_months(&comm_mth,-4);
define trx_s_date = add_months(&comm_mth,-4);
define trx_e_date = add_months(&comm_mth,-3)-1;
---------------------Basic Telelcom -----------------------------------------------------------------------------------
prompt 'Step B_BM_CRC_COMM_004A01_T : [Prepare the base roam comm case  ] ';

insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_004A01_T
(    trx_mth
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
    ,json_rmk
) 
-------New base telecom cases -----
select     
    t.trx_mth
    ,t.comm_mth
    ,t.comm_mth as orig_comm_mth
    ,t.case_id
    ,t.case_type
    ,t.ld_inv_num
    ,t.ld_start_date
    ,slm.cust_num as lm_cust_num
    ,slm.subr_num as lm_subr_num
    ,slm.ld_expired_date as lm_ld_expired_date
    ,slo.ld_expired_date as orig_ld_expired_date
    ,t.ld_cd
    ,t1.comm_rate
    ,0 as seq
    ,' ' as json_rmk
from  ${etlvar::TMPDB}.B_BM_CRC_COMM_003_T t1
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_001_T t
  on t1.case_id = t.case_id
left outer join prd_adw.subr_ld_hist slo
 on t.ld_inv_num = slo.inv_num
and t.ld_start_date between slo.start_date and slo.end_date
left outer join prd_adw.subr_ld_hist slm
        on t.ld_inv_num = slm.inv_num
and &comm_mth - 1 between slm.start_date and slm.end_date
where t1.case_type in ('CASE_BT_5G','CASE_BT_NON5G')
union 
---------carry forward cases ----------------
 select
    h.trx_mth
    ,&comm_mth
    ,h.orig_comm_mth as orig_comm_mth
    ,h.case_id
    ,h.case_type
    ,h.ld_inv_num
    ,h.ld_start_date
    ,slm.cust_num as lm_cust_num
    ,slm.subr_num as lm_subr_num
    ,slm.ld_expired_Date
    ,h.orig_ld_expired_date
    ,h.ld_cd
    ,case when ov.case_id is not null then ov.comm_rate else h.comm_rate end comm_rate
    ,h.seq + 1 as seq
    ,h.json_rmk||case when ov.case_id is not null then ',"OVERR_COMM_RATE":"Y"' else ' ' end json_rmk
from ${etlvar::ADWDB}.BM_CRC_COMM_ROAM_H h
left outer join  prd_adw.subr_ld_hist slm
        on h.ld_inv_num = slm.inv_num
        and &comm_mth - 1 between slm.start_date and slm.end_date
left outer join ${etlvar::ADWDB}.BM_CRC_COMM_OVERR_ROAM_H ov
    on h.trx_mth = ov.trx_mth
   and h.case_id = ov.case_id
   and ov.comm_mth = add_months(&comm_mth,-1)
where 
        h.comm_mth = add_months(&comm_mth,-1)
and h.roam_comm_status not like  'END%';
commit;


prompt 'Step B_BM_CRC_COMM_004B01_T : [Prepare the base roam comm case  ] ';
----- For first month 3M = LM
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_004B01_T
(
        case_id
        ,NET_ROAM_REV_AMT_1M
        ,NET_ROAM_REV_AMT_2M
        ,NET_ROAM_REV_AMT_3M
        ,NET_ROAM_REV_AMT_LM
)
Select t.case_id            
            ,nvl(sum(decode(pr.trx_month, to_char(add_months(t.trx_mth,1),'yyyymm'), pr.NET_OUT_ROAM_REV,0)),0) NET_ROAM_REV_AMT_1M
            ,nvl(sum(decode(pr.trx_month, to_char(add_months(t.trx_mth,2),'yyyymm'), pr.NET_OUT_ROAM_REV,0)),0) NET_ROAM_REV_AMT_2M
            ,nvl(sum(decode(pr.trx_month, to_char(add_months(t.trx_mth,3),'yyyymm'), pr.NET_OUT_ROAM_REV,0)),0) NET_ROAM_REV_AMT_3M
            ,nvl(sum(decode(pr.trx_month, to_char(add_months(t.comm_mth,-1),'yyyymm'), pr.NET_OUT_ROAM_REV,0)),0) NET_ROAM_REV_AMT_LM 
    from ${etlvar::TMPDB}.B_BM_CRC_COMM_004A01_T t
    left outer join PRD_BIZ_SUMM_VW.VW_PRF_SUBR_INFO pr
        on t.lm_subr_num = pr.subr_num
        and pr.trx_month between to_char(t.trx_mth,'yyyymm') and to_char(add_months(t.trx_mth ,6),'yyyymm')    
    group by t.case_id;
commit;

prompt 'Step B_BM_CRC_COMM_004B02_T : [calcualte comm roam comm case  ] ';
insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_004B02_T
(
        case_id
        ,comm_rate
        ,seq
        ,net_roam_rev_amt_1m
        ,net_roam_rev_amt_2m
        ,net_roam_rev_amt_3m
        ,net_roam_rev_amt_lm
        ,ttl_roam_comm_amt
)
select t.case_id
        ,t.comm_rate
        ,t.seq 
        ,case when seq = 0 then f.net_roam_rev_amt_1m else 0  end  net_roam_rev_amt_1m 
        ,case when seq = 0 then f.net_roam_rev_amt_2m else 0  end  net_roam_rev_amt_2m
        ,case when seq = 0 then f.net_roam_rev_amt_3m else 0  end  net_roam_rev_amt_3m
        ,f.net_roam_rev_amt_lm
        ,case when t.seq = 0 
          then (f.net_roam_rev_amt_1m + f.net_roam_rev_amt_2m + f.net_roam_rev_amt_3m) * t.comm_rate
          else f.net_roam_rev_amt_lm * t.comm_rate
         end as ttl_roam_comm_amt
 from ${etlvar::TMPDB}.B_BM_CRC_COMM_004A01_T t
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_004B01_T f
        on t.case_id = f.case_id;

prompt 'Step B_BM_CRC_COMM_004C01_T  : [Check roam commission status]';

insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_004C01_T
(
        case_id
        ,roam_comm_status
)
select t.case_id
        --- change to use orig ld expired date
        ,case 
            when t.comm_rate = 0  then
            'END_COMM_RATE_ZERO'
            when t.orig_ld_expired_date between add_months(&comm_mth ,-1) and &comm_mth -1 then
            'END_LD_EXPIRY'
            when  h.subr_sw_off_date between add_months(&comm_mth ,-1) and &comm_mth -1 then
            'END_PROFILE_SW_OFF'
            when nvl(prv.prv_case_id,' ')<>' ' and prv.prv_case_src ='PRV_RETENT' then
            'END_RENEW_OLD_CASE'
          else ' '
        end
      from ${etlvar::TMPDB}.B_BM_CRC_COMM_004A01_T t
    left outer join prd_adw.subr_ld_hist sl
        on t.ld_inv_num = sl.inv_num
        and &comm_mth - 1 between sl.start_date and sl.end_date
    left outer join prd_adw.subr_info_hist h
    on sl.subr_num = h.subr_num
    and sl.cust_num = h.cust_num
        and &comm_mth - 1 between h.start_date and h.end_date
    ------End all carryforward case when found current
    left outer join (
        select distinct prv_case_id,prv_case_src  from (
                select prv_case_id,prv_case_src from ${etlvar::TMPDB}.B_BM_CRC_COMM_002_T
                union all
                select prv_case_id,prv_case_src from ${etlvar::TMPDB}.B_BM_CRC_COMM_003_T
        )
    ) prv
        on t.case_id = prv.prv_case_id
    where sl.ld_expired_date between add_months(&comm_mth ,-1) and &comm_mth -1
    or h.subr_sw_off_date between add_months(&comm_mth ,-1) and &comm_mth -1
    or nvl(prv.prv_case_id,' ')<> ' '
    or t.comm_rate = 0 ;
commit;


insert into ${etlvar::TMPDB}.B_BM_CRC_COMM_004_T
(
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
)
select 
    t.trx_mth
    ,t.comm_mth
    ,t.orig_comm_mth
    ,t.case_id
    ,t.case_type
    ,t.ld_inv_num
    ,t.ld_start_date
    ,t.lm_cust_num
    ,t.lm_subr_num
    ,t.lm_ld_expired_date
    ,t.orig_ld_expired_date
    ,t.ld_cd
    ,t.comm_rate
    ,t.seq
    ,b.net_roam_rev_amt_1m
    ,b.net_roam_rev_amt_2m
    ,b.net_roam_rev_amt_3m
    ,b.net_roam_rev_amt_lm
    ,case when c.roam_comm_status =' ' then 0 else b.ttl_roam_comm_amt end ttl_roam_comm_amt
    ,nvl(c.roam_comm_status,' ')
    ,t.json_rmk
from  ${etlvar::TMPDB}.B_BM_CRC_COMM_004A01_T t
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_004B02_T b
  on t.case_id = b.case_id 
left outer join ${etlvar::TMPDB}.B_BM_CRC_COMM_004C01_T c
  on t.case_id = c.case_id ;
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

