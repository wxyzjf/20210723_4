/opt/etl/prd/etl/APP/ADW/Y_B_RETENT_CORP_COMM_RPT/bin> cat b_retent_corp_comm_rpt0000.pl
######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29 
#
#
######################################################


##my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/O_B_RETENT_CORP_COMM_RPT/bin/master_dev.pl";
require $ETLVAR;

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

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_001A_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_001B_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_001_T');
-----
define comm_mth=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_mth=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),-4);
define rpt_s_date=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),-4);
define rpt_e_date=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),-3)-1;


prompt 'b_retent_corp_comm_001A_t [Preparing base profile ]';
insert into mig_adw.b_retent_corp_comm_001A_t
(   rpt_mth
    ,comm_mth
    ,case_id 
    ,cust_num
    ,subr_num
    ,ld_inv_num
    ,ld_cd
    ,ld_mkt_cd
    ,ld_start_date
    ,ld_expired_date
    ,waived_flg
    ,bill_cycle
    ,subr_sw_on_date
    ,subr_stat_cd
    ,rate_plan_cd
    ,calc_rmk)
select
     &rpt_mth
    ,&comm_mth
    ,'C'||to_char(&rpt_mth,'YYMM')||'_'||sl.inv_num||'_'||row_number() over ( order by sl.inv_num) as case_id
    ,sl.cust_num
    ,sl.subr_num
    ,sl.inv_num as ld_inv_num
    ,sl.ld_cd
    ,sl.mkt_cd as ld_mkt_cd
    ,sl.ld_start_date
    ,sl.ld_expired_date
    ,sl.waived_flg
    ,nvl(nvl(s.bill_cycle,s2.bill_cycle),0) as bill_cycle
    ,nvl(nvl(s.subr_sw_on_date,s2.subr_sw_on_date),date '2999-12-31') subr_sw_on_date 
    ,nvl(nvl(s.subr_stat_cd,s2.subr_stat_cd),' ') subr_stat_cd
    ,nvl(nvl(s.rate_plan_cd,s2.rate_plan_cd),' ') rate_plan_cd
    ,' ' as calc_rmk
from prd_adw.subr_ld_hist sl
left outer join prd_adw.subr_info_hist s
  on sl.cust_num = s.cust_num
  and sl.subr_num =s.subr_num
  and sl.ld_start_date between s.start_date and s.end_date
left outer join prd_adw.subr_info_hist s2
  on sl.cust_num = s2.cust_num
  and sl.subr_num =s2.subr_num
  and &rpt_e_date between s2.start_date and s2.end_date
where sl.ld_start_date between &rpt_s_date and &rpt_e_date
  and sl.void_flg <> 'Y'
  and sl.ld_expired_date > &rpt_e_date
  and sl.mkt_cd in(select mkt_cd from prd_adw.mkt_ref_vw v where ld_revenue='P')
  and sl.inv_num not in (
   select r.inv_num
   from prd_adw.pos_return_header r 
   where r.trx_date between &rpt_s_date and &comm_mth
  );
commit;

prompt 'b_retent_corp_comm_001b_T [Preparing base profile ]';
insert into mig_adw.b_retent_corp_comm_001b_t
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
        from mig_adw.b_retent_corp_comm_001a_t t
            ,prd_adw.subr_ld_hist o
        where t.subr_num = o.subr_num
         and t.cust_num = o.cust_num
         --and t.ld_start_date > o.ld_expired_date
         ---- Suppose the old ld valid in last month
         and &rpt_s_date - 1  between o.ld_start_date and o.ld_expired_date
         and o.mkt_cd in (select mkt_cd from prd_adw.mkt_ref_vw v where ld_revenue='P')
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


insert into  mig_adw.b_retent_corp_comm_001_t
(
        rpt_mth
        ,comm_mth
        ,case_id
        ,cust_num
        ,subr_num
        ,ld_inv_num
        ,ld_cd
        ,ld_mkt_cd
        ,ld_start_date
        ,ld_expired_date
        ,rate_plan_cd
        ,bill_rate
        ,old_ld_inv_num
        ,old_ld_cd
        ,old_ld_mkt_cd
        ,old_ld_start_date
        ,old_ld_expired_date
        ,old_rate_plan_cd
        ,old_bill_rate
        ,waived_flg
        ,bill_cycle
        ,subr_sw_on_date
        ,subr_stat_cd
        ,calc_rmk
) select 
         t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.cust_num
        ,t.subr_num
        ,t.ld_inv_num
        ,t.ld_cd
        ,t.ld_mkt_cd
        ,t.ld_start_date
        ,t.ld_expired_date
        ,t.rate_plan_cd
        ,nvl(rn.bill_rate,0)
        ,nvl(o.old_ld_inv_num,' ')
        ,nvl(o.old_ld_cd,' ')
        ,nvl(o.old_ld_mkt_cd,' ')
        ,nvl(o.old_ld_start_date,date '1900-01-01')
        ,nvl(o.old_ld_expired_date,date '1900-01-01')
        ,nvl(o.old_rate_plan_cd,' ')
        ,nvl(ro.bill_rate,0)
        ,t.waived_flg
        ,t.bill_cycle
        ,t.subr_sw_on_date
        ,t.subr_stat_cd
        ,t.calc_rmk
from  mig_adw.b_retent_corp_comm_001a_t t
left outer join mig_adw.b_retent_corp_comm_001b_t o
        on t.case_id = o.case_id
left outer join prd_adw.bill_serv_ref rn
        on t.rate_plan_cd = rn.bill_serv_cd
        and &rpt_e_date  between rn.eff_start_date and rn.eff_end_date
left outer join prd_adw.bill_serv_ref ro
        on t.rate_plan_cd = ro.bill_serv_cd
        and &rpt_e_date  between ro.eff_start_date and ro.eff_end_date
-----not sw on current month-----
where t.subr_sw_on_date not between &rpt_s_date and &rpt_e_date;
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

