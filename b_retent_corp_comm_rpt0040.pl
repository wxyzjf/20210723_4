######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29 
#
#
######################################################


my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
##my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_B_RETENT_CORP_COMM_RPT/bin/master_dev.pl";
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
---------------------------------------------------------------------------------------------------------

set linesize 2000
alter session force parallel dml parallel 30;

set define on;
define tx_date = trunc(to_date('$etlvar::TXDATE','YYYY-MM-DD'),'MM');
define comm_mth = add_months(&tx_date,-1);
define rpt_mth = add_months(&comm_mth,-3);
define rpt_s_date = add_months(&comm_mth,-3);
define rpt_e_date = add_months(&comm_mth,-2)-1;

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_004A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::TMPDB}',p_table_name=>'B_RETENT_CORP_COMM_004B01_T');

DELETE FROM ${etlvar::ADWDB}.RETENT_CORP_COMM_H where rpt_mth = &rpt_mth;


prompt 'b_retent_corp_comm_004A01_t [Preparing all figure before calculation]';
insert into ${etlvar::TMPDB}.B_RETENT_CORP_COMM_004A01_T t
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
  ,old_ld_inv_num
  ,old_ld_cd
  ,old_ld_mkt_cd
  ,old_ld_start_date
  ,old_ld_expired_date
  ,bill_cycle
  ,subr_sw_on_date
  ,subr_stat_cd
  ,rate_plan_cd
  ,bill_rate
  ,old_rate_plan_cd
  ,old_bill_rate
  ,bill_inv_list
  ,rebate_amt
  ,hs_subsidy_amt
  ,old_bill_inv_list
  ,old_rebate_amt
  ,old_hs_subsidy_amt
  ,salesman_cd
  ,sales_team
  ,rebate_flg
  ,old_rebate_flg
  ,bm_cat
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
  ,t.ld_expired_date
  ,t.old_ld_inv_num
  ,t.old_ld_cd
  ,t.old_ld_mkt_cd
  ,t.old_ld_start_date
  ,t.old_ld_expired_date
  ,t.bill_cycle
  ,t.subr_sw_on_date
  ,t.subr_stat_cd
  ,t.rate_plan_cd
  ,t.bill_rate
  ,t.old_rate_plan_cd
  ,t.old_bill_rate
  ,nvl(n.bill_inv_list, ' ')
  ,nvl(n.rebate_amt,0)
  ,nvl(n.hs_subsidy_amt,0)
  ,nvl(o.old_bill_inv_list, ' ')
  ,nvl(o.old_rebate_amt,0)
  ,nvl(o.old_hs_subsidy_amt,0)
  ,nvl(n.salesman_cd, ' ')
  ,nvl(n.sales_team, ' ')
  ,nvl(n.rebate_flg,' ')
  ,nvl(o.old_rebate_flg,' ')
  ,case when bmc.bm_cat is null
        then nvl(bmc2.bm_cat,' ')
   else bmc.bm_cat
   end as bm_cat
  --,nvl(bmc.bm_cat,' ')
  ,nvl(t.json_rmk,' ')||nvl(n.json_rmk,' ')||nvl(o.json_rmk,' ')
from ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001_T t
---new rebate hs_subsidy
left outer join ${etlvar::TMPDB}.B_RETENT_CORP_COMM_002_T n 
        on t.case_id = n.case_id
---old rebate hs_subsidy
left outer join ${etlvar::TMPDB}.B_RETENT_CORP_COMM_003_T o
        on t.case_id = o.case_id
left outer join ${etlvar::ADWDB}.RETENT_CORP_COMM_MKT_REF bmc
        on t.ld_mkt_cd = bmc.mkt_cd
           and bmc.COMM_MTH = &comm_mth
left outer join ${etlvar::ADWDB}.RETENT_CORP_COMM_MKT_REF bmc2
        on t.pos_mkt_cd = bmc2.mkt_cd
           and bmc2.COMM_MTH = &comm_mth
left outer join (
---Bm cat file base on invoice date
        Select tr.case_id,b.bm_cat
        from ${etlvar::TMPDB}.B_RETENT_CORP_COMM_001_T tr
            ,prd_adw.pos_inv_header ph
            ,${etlvar::ADWDB}.RETENT_CORP_COMM_MKT_REF b
        where tr.ld_inv_num = ph.inv_num
        and tr.ld_mkt_cd = b.mkt_cd
        and ph.inv_date between b.eff_s_date and b.eff_e_date
        and b.comm_mth = &comm_mth
)bmc on  t.case_id = bmc.case_id
where nvl(n.salesman_cd ,' ') <> ' ';

prompt 'Step Override the rebate and  bill_rate for others ';

update  ${etlvar::TMPDB}.B_RETENT_CORP_COMM_004A01_T t
set (t.old_rebate_amt,t.old_bill_rate,t.rebate_amt,t.bill_rate,rebate_flg,old_rebate_flg,old_ld_inv_num) = (
        select  r.overr_rebate_amt 
                ,r.overr_bill_rate
                ,r.overr_rebate_amt 
                ,r.overr_bill_rate
                ,'INV_REBATE'
                ,'INV_REBATE'
                ,'OVERRIDE_OLD'
        from ${etlvar::ADWDB}.RETENT_CORP_COMM_MKT_REF r
        where t.ld_mkt_cd = r.mkt_cd 
         and nvl(r.override_flg,' ') ='Y'
         and r.comm_mth = &comm_mth
)where t.ld_mkt_cd 
in ( select rr.mkt_cd from ${etlvar::ADWDB}.RETENT_CORP_COMM_MKT_REF rr where nvl(rr.override_flg,' ') ='Y' and rr.comm_mth = &comm_mth) ;

commit;

prompt 'Step B_RETENT_CORP_COMM_004B01_T : [ handling the calculation value ]';
declare
    cursor cur_t is
        select * from ${etlvar::TMPDB}.B_RETENT_CORP_COMM_004A01_T ;
    rs_sr  ${etlvar::TMPDB}.B_RETENT_CORP_COMM_004A01_T%ROWTYPE;
    rs_ta  ${etlvar::TMPDB}.B_RETENT_CORP_COMM_004B01_T%ROWTYPE;
    commit_cnt integer;
    
begin
    commit_cnt:=0;
    open cur_t;
    loop
        fetch cur_t into rs_sr;
        exit when cur_t%notfound;
    rs_ta.rpt_mth := rs_sr.rpt_mth;
    rs_ta.comm_mth := rs_sr.comm_mth;
    rs_ta.case_id := rs_sr.case_id;
    rs_ta.cust_num := rs_sr.cust_num;
    rs_ta.subr_num := rs_sr.subr_num;
    rs_ta.ld_inv_num := rs_sr.ld_inv_num;
    rs_ta.ld_cd := rs_sr.ld_cd;
    rs_ta.ld_mkt_cd := rs_sr.ld_mkt_cd;
    rs_ta.ld_start_date := rs_sr.ld_start_date;
    rs_ta.ld_expired_date := rs_sr.ld_expired_date;
    rs_ta.pos_inv_num := rs_sr.pos_inv_num;
    rs_ta.pos_mkt_cd := rs_sr.pos_mkt_cd;
    rs_ta.pos_inv_date := rs_sr.pos_inv_date;
    rs_ta.pos_inv_tag := rs_sr.pos_inv_tag;
    rs_ta.pos_ld_cd := rs_sr.pos_ld_cd;
    rs_ta.old_ld_inv_num := rs_sr.old_ld_inv_num;
    rs_ta.old_ld_cd := rs_sr.old_ld_cd;
    rs_ta.old_ld_mkt_cd := rs_sr.old_ld_mkt_cd;
    rs_ta.old_ld_start_date := rs_sr.old_ld_start_date;
    rs_ta.old_ld_expired_date := rs_sr.old_ld_expired_date;
--    rs_ta.waived_flg := rs_sr.waived_flg;
    rs_ta.bill_cycle := rs_sr.bill_cycle;
    rs_ta.subr_sw_on_date := rs_sr.subr_sw_on_date;
    rs_ta.subr_stat_cd := rs_sr.subr_stat_cd;
    rs_ta.bill_inv_list := rs_sr.bill_inv_list;
    rs_ta.rebate_amt := rs_sr.rebate_amt;
    rs_ta.hs_subsidy_amt := rs_sr.hs_subsidy_amt;
    rs_ta.old_bill_inv_list := rs_sr.old_bill_inv_list;
    rs_ta.old_rebate_amt := rs_sr.old_rebate_amt;
    rs_ta.old_hs_subsidy_amt := rs_sr.old_hs_subsidy_amt;
    rs_ta.rate_plan_cd := rs_sr.rate_plan_cd;
    rs_ta.bill_rate := rs_sr.bill_rate;
    rs_ta.old_rate_plan_cd := rs_sr.old_rate_plan_cd;
    rs_ta.old_bill_rate := rs_sr.old_bill_rate;
    rs_ta.bm_cat := rs_sr.bm_cat;
    rs_ta.rebate_flg:= rs_sr.rebate_flg;
    rs_ta.old_rebate_flg:= rs_sr.old_rebate_flg;
    rs_ta.salesman_cd := rs_sr.salesman_cd;
    rs_ta.json_rmk := rs_sr.json_rmk;
------ calculation column ----
    rs_ta.contract_mth := case when round((rs_sr.ld_expired_date - rs_sr.ld_start_date +1)/365*12,1) = 0 then 1 else round((rs_sr.ld_expired_date - rs_sr.ld_start_date+1)/365*12,1)  end;
    rs_ta.old_contract_mth := case when rs_sr.old_ld_inv_num <> ' ' and rs_sr.old_ld_expired_date <> rs_sr.old_ld_start_date then 
                                round((rs_sr.old_ld_expired_date - rs_sr.old_ld_start_date + 1)/365 * 12,1) 
                             else 
                                1
                             end; 
    rs_ta.chg_plan_flg := case when rs_sr.old_ld_expired_date >rs_sr.ld_start_date 
                         then 'Y' else 'N' end ;
    rs_ta.remain_contract_mth :=  case when rs_ta.chg_plan_flg ='Y' and rs_sr.old_ld_inv_num <> ' ' 
                                        and nvl(rs_sr.OLD_LD_EXPIRED_DATE,date '1900-01-01') > rs_sr.LD_START_DATE
                                       then round((rs_sr.OLD_LD_EXPIRED_DATE - rs_sr.LD_START_DATE + 1) / 365 * 12 , 1)
                                else 0 end ;

-----notice rebate_amt is a negative figure----
    rs_ta.tcv := (rs_ta.bill_rate + case when rs_ta.rebate_flg in( 'INV_REBATE' ,'OVR_REBATE') then rs_ta.rebate_amt else 0 end  - rs_ta.hs_subsidy_amt / rs_ta.contract_mth) *  rs_ta.contract_mth 
                + case when rs_ta.rebate_flg = 'MKT_REBATE' then rs_ta.rebate_amt else 0 end
                - case when rs_ta.old_ld_inv_num <> ' ' then 
                        (rs_ta.old_bill_rate + case when rs_ta.old_rebate_flg in( 'INV_REBATE','OVR_REBATE') then rs_ta.old_rebate_amt else 0 end  
                        - rs_ta.old_hs_subsidy_amt / rs_ta.old_contract_mth
                        + case when rs_ta.old_rebate_flg = 'MKT_REBATE' and  rs_ta.remain_contract_mth > 0 
                                then rs_ta.old_rebate_amt else 0 end/rs_ta.old_contract_mth
                        ) 
                        * rs_ta.remain_contract_mth
                  else 0 end ;
    insert into ${etlvar::TMPDB}.B_RETENT_CORP_COMM_004B01_T
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
    ,old_ld_inv_num
    ,old_ld_cd
    ,old_ld_mkt_cd
    ,old_ld_start_date
    ,old_ld_expired_date
    --,waived_flg
    ,bill_cycle
    ,subr_sw_on_date
    ,subr_stat_cd
    ,bill_inv_list
    ,rebate_amt
    ,hs_subsidy_amt
    ,old_bill_inv_list
    ,old_rebate_amt
    ,old_hs_subsidy_amt
    ,rate_plan_cd
    ,bill_rate
    ,old_rate_plan_cd
    ,old_bill_rate
    ,bm_cat
    ,contract_mth
    ,old_contract_mth
    ,chg_plan_flg
    ,remain_contract_mth
    ,tcv
    ,rebate_flg
    ,old_rebate_flg
    ,salesman_cd
    ,json_rmk
        )
    values(
     rs_ta.rpt_mth
    ,rs_ta.comm_mth
    ,rs_ta.case_id
    ,rs_ta.cust_num
    ,rs_ta.subr_num
  ,rs_ta.pos_inv_num
  ,rs_ta.pos_mkt_cd
  ,rs_ta.pos_inv_date
  ,rs_ta.pos_inv_tag
  ,rs_ta.pos_ld_cd
    ,rs_ta.ld_inv_num
    ,rs_ta.ld_cd
    ,rs_ta.ld_mkt_cd
    ,rs_ta.ld_start_date
    ,rs_ta.ld_expired_date
    ,rs_ta.old_ld_inv_num
    ,rs_ta.old_ld_cd
    ,rs_ta.old_ld_mkt_cd
    ,rs_ta.old_ld_start_date
    ,rs_ta.old_ld_expired_date
    --,rs_ta.waived_flg
    ,rs_ta.bill_cycle
    ,rs_ta.subr_sw_on_date
    ,rs_ta.subr_stat_cd
    ,rs_ta.bill_inv_list
    ,rs_ta.rebate_amt
    ,rs_ta.hs_subsidy_amt
    ,rs_ta.old_bill_inv_list
    ,rs_ta.old_rebate_amt
    ,rs_ta.old_hs_subsidy_amt
    ,rs_ta.rate_plan_cd
    ,rs_ta.bill_rate
    ,rs_ta.old_rate_plan_cd
    ,rs_ta.old_bill_rate
    ,rs_ta.bm_cat
    ,rs_ta.contract_mth
    ,rs_ta.old_contract_mth
    ,rs_ta.chg_plan_flg
    ,rs_ta.remain_contract_mth
    ,rs_ta.tcv
    ,rs_ta.rebate_flg
    ,rs_ta.old_rebate_flg
    ,rs_ta.salesman_cd
    ,rs_ta.json_rmk
        );
        commit_cnt := commit_cnt +1;
    if mod(commit_cnt,5000) = 0 then
                commit;
    end if;
    end loop;
    commit;
    close cur_t;
end;
/
commit;

insert into ${etlvar::ADWDB}.RETENT_CORP_COMM_H
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
    ,old_ld_inv_num
    ,old_ld_cd
    ,old_ld_mkt_cd
    ,old_ld_start_date
    ,old_ld_expired_date
    --,waived_flg
    ,bill_cycle
    ,subr_sw_on_date
    ,subr_stat_cd
    ,bill_inv_list
    ,rebate_amt
    ,hs_subsidy_amt
    ,old_bill_inv_list
    ,old_rebate_amt
    ,old_hs_subsidy_amt
    ,rate_plan_cd
    ,bill_rate
    ,old_rate_plan_cd
    ,old_bill_rate
    ,bm_cat
    ,contract_mth
    ,old_contract_mth
    ,chg_plan_flg
    ,remain_contract_mth
    ,tcv
    ,rebate_flg
    ,old_rebate_flg
    ,salesman_cd
    ,json_rmk)
select 
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
    ,old_ld_inv_num
    ,old_ld_cd
    ,old_ld_mkt_cd
    ,old_ld_start_date
    ,old_ld_expired_date
    --,waived_flg
    ,bill_cycle
    ,subr_sw_on_date
    ,subr_stat_cd
    ,bill_inv_list
    ,rebate_amt
    ,hs_subsidy_amt
    ,old_bill_inv_list
    ,old_rebate_amt
    ,old_hs_subsidy_amt
    ,rate_plan_cd
    ,bill_rate
    ,old_rate_plan_cd
    ,old_bill_rate
    ,bm_cat
    ,contract_mth
    ,old_contract_mth
    ,chg_plan_flg
    ,remain_contract_mth
    ,tcv
    ,rebate_flg
    ,old_rebate_flg
    ,salesman_cd
    ,json_rmk
from  ${etlvar::TMPDB}.B_RETENT_CORP_COMM_004B01_T;
commit;


quit;
---------------------------------------------------------
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

